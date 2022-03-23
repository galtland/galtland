// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;

use anyhow::Context;
use bytes::BytesMut;
use itertools::Itertools;
use libp2p::request_response::ResponseChannel;
use libp2p::PeerId;
use log::debug;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::mpsc::{self};
use tokio::sync::{oneshot, Mutex};

use super::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::KademliaRecord;
use crate::protocols::simple_file_exchange::{SimpleFileRequest, SimpleFileResponse};
use crate::utils;

#[derive(Debug)]
pub struct RespondSimpleFileInfo {
    pub peer: PeerId,
    pub request: SimpleFileRequest,
    pub filename: String,
    pub channel: ResponseChannel<Result<SimpleFileResponse, String>>,
}

#[derive(Debug)]
pub struct PublishSimpleFileInfo {
    pub filename: String,
    pub hash: Vec<u8>,
    pub sender: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug)]
pub struct GetSimpleFileInfo {
    pub hash: Vec<u8>,
    pub sender: tokio::sync::mpsc::Sender<
        Result<Result<SimpleFileResponse, String>, libp2p::request_response::OutboundFailure>,
    >,
}

//FIXME: too many error being converted to string in this function
pub(crate) async fn handle_get(
    network: NetworkBackendClient,
    info: GetSimpleFileInfo,
) -> anyhow::Result<()> {
    let GetSimpleFileInfo { hash, sender } = info;
    debug!("handle_get {:?}", hash);

    let peers = match network.clone().get_providers(hash.clone()).await {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            sender
                .send(Ok(Err(format!("get providers error: {e}"))))
                .await
                .context("Expected receiver to be still up")?;
            return Ok(());
        }
        Err(e) => {
            sender
                .send(Ok(Err(format!("error while getting providers: {e}"))))
                .await
                .context("Expected receiver to be still up")?;
            return Ok(());
        }
    };
    let peer = match peers.iter().next() {
        Some(p) => p,
        None => {
            sender
                .send(Ok(Err("No peers for file".to_string())))
                .await
                .context("Expected receiver to be still up")?;
            return Ok(());
        }
    };

    let file_request = SimpleFileRequest { key: hash };
    loop {
        match network
            .request_simple_file(file_request.clone(), *peer)
            .await
        {
            Ok(Ok(Ok(r))) => {
                let eof = r.eof;
                sender
                    .send(Ok(Ok(r)))
                    .await
                    .context("Expected receiver to be still up")?;
                if eof {
                    return Ok(());
                }
            }
            Ok(Ok(Err(e))) => {
                sender
                    .send(Ok(Err(format!("got a string error: {e}"))))
                    .await
                    .context("Expected receiver to be still up")?;
                return Ok(());
            }
            Ok(Err(e)) => {
                sender
                    .send(Ok(Err(format!("got a outbound failure: {e}"))))
                    .await
                    .context("Expected receiver to be still up")?;
                return Ok(());
            }
            Err(e) => {
                sender
                    .send(Ok(Err(e.to_string())))
                    .await
                    .context("Expected receiver to be still up")?;
                return Ok(());
            }
        }
    }
}

pub(crate) async fn handle_publish(
    network: NetworkBackendClient,
    info: PublishSimpleFileInfo,
) -> anyhow::Result<()> {
    let PublishSimpleFileInfo {
        filename,
        hash,
        sender,
    } = info;
    debug!("handle_publish {:?}", hash);

    let record = KademliaRecord::new_simple_file_record(&hash);
    let put_record_result = network.put_record(record).await;
    let start_providing_result = network
        .start_providing_simple_file(hash, filename.clone())
        .await;
    log::debug!("put_record_result {:?}", put_record_result);
    log::debug!("start_providing_result {:?}", start_providing_result);

    let result = if put_record_result.is_ok() && start_providing_result.is_ok() {
        Ok(())
    } else {
        let a = put_record_result
            .err()
            .map(|e| format!("Error putting record {:?}", e))
            .into_iter();
        let b = start_providing_result
            .err()
            .map(|e| format!("Error start providing {:?}", e))
            .into_iter();

        let c: String = Itertools::intersperse(a.chain(b), '\n'.to_string()).collect();
        Err(c)
    };
    log::debug!("publish result {:?}", result);

    if sender.send(result).is_err() {
        anyhow::bail!("Failed to send result")
    }
    Ok(())
}

pub(crate) async fn handle_respond(
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    info: RespondSimpleFileInfo,
) -> anyhow::Result<()> {
    let RespondSimpleFileInfo {
        peer,
        request,
        filename,
        channel,
    } = info;
    debug!("handle_respond {} {:?}", peer, request);
    let receiver = shared_state
        .active_file_transfers
        .lock()
        .await
        .entry((peer, request.key.clone()))
        .or_insert_with(|| {
            let (sender, receiver) = tokio::sync::mpsc::channel(2);
            let filename = filename.clone();
            utils::spawn_and_log_error(respond_simple_file_daemon(
                network.clone(),
                sender,
                filename,
            ));
            Arc::new(Mutex::new(receiver))
        })
        .clone();
    tokio::task::yield_now().await;
    match receiver.lock().await.recv().await {
        Some(Ok(response)) => {
            log::info!("preparing to send new response");
            if response.eof {
                shared_state
                    .active_file_transfers
                    .lock()
                    .await
                    .remove(&(peer, request.key));
            }
            network.respond_simple_file(Ok(response), channel).await
        }
        Some(Err(e)) => {
            shared_state
                .active_file_transfers
                .lock()
                .await
                .remove(&(peer, request.key));
            network.respond_simple_file(Err(e), channel).await
        }
        None => {
            shared_state
                .active_file_transfers
                .lock()
                .await
                .remove(&(peer, request.key));
            network
                .respond_simple_file(Err("Unexpected EOF".to_string()), channel)
                .await
        }
    }?;
    Ok(())
}

async fn respond_simple_file_daemon(
    _network: NetworkBackendClient,
    sender: mpsc::Sender<Result<SimpleFileResponse, String>>,
    filename: String,
) -> anyhow::Result<()> {
    let f = File::open(filename)
        .await
        .context("expected file to be accessible")?;
    let mut f = BufReader::with_capacity(8192, f); // TODO: check if makes any difference
    let frame_size: usize = 16 * 1024;
    let max_bytes_per_packet: usize = 1024 * 1024;
    let max_to_read = max_bytes_per_packet - frame_size;
    let mut bytes = BytesMut::with_capacity(max_bytes_per_packet);
    let mut eof = false;
    while !eof {
        bytes.clear(); // TODO: use freeze, etc
        loop {
            match f.read_buf(&mut bytes).await {
                Ok(r) => {
                    if r == 0 {
                        eof = true;
                        break;
                    }
                    if bytes.len() >= max_to_read {
                        break;
                    }
                }
                Err(e) => {
                    let e = e.to_string();
                    sender
                        .send(Err(e))
                        .await
                        .context("expected receiver able to receive data")?;
                    return Ok(());
                }
            }
        }
        log::info!("Read vector of {} bytes", bytes.len());
        let data = bytes.to_vec();
        let response = SimpleFileResponse { eof: false, data };
        sender
            .send(Ok(response))
            .await
            .context("expected receiver able to receive data")?;
        tokio::task::yield_now().await;
    }
    let last_response = SimpleFileResponse {
        eof: true,
        data: Default::default(),
    };
    sender
        .send(Ok(last_response))
        .await
        .context("expected receiver able to receive data")?;
    Ok(())
}
