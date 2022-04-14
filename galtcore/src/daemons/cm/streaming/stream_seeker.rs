// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

use anyhow::Context;
use libp2p::kad::record::Key;
use libp2p::request_response::OutboundFailure;
use rayon::prelude::*;
use tokio::sync::oneshot;

use super::stream_publisher::StreamPublisherClient;
use crate::daemons::cm::streaming::{peer_seeker, stream_publisher};
use crate::daemons::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::{KademliaRecord, StreamingRecord};
use crate::protocols::media_streaming::{
    StreamSeekType, StreamingKey, StreamingRequest, StreamingResponse,
};
use crate::protocols::NodeIdentity;
use crate::utils;

pub(crate) async fn launch_daemon(
    identity: NodeIdentity,
    streaming_key: StreamingKey,
    sender: oneshot::Sender<anyhow::Result<StreamPublisherClient>>,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
) -> anyhow::Result<()> {
    match shared_state
        .stream_seeker
        .lock()
        .await
        .entry(streaming_key.clone())
    {
        std::collections::hash_map::Entry::Occupied(_) => {
            todo!("not sure what to do here, probably just get daemon sender and send");
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            let (abort_sender, abort_receiver) = oneshot::channel();
            utils::spawn_and_log_error({
                let shared_state = shared_state.clone();
                async move {
                    let r = _stream_seeker_daemon(
                        identity,
                        streaming_key.clone(),
                        shared_state.clone(),
                        network,
                        sender,
                        abort_receiver,
                    )
                    .await;
                    shared_state
                        .stream_seeker
                        .lock()
                        .await
                        .remove(&streaming_key);
                    r
                }
            });
            entry.insert(abort_sender);
            tokio::task::yield_now().await;
        }
    };
    Ok(())
}

async fn _stream_seeker_daemon(
    identity: NodeIdentity,
    streaming_key: StreamingKey,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    initializer: oneshot::Sender<anyhow::Result<StreamPublisherClient>>,
    mut abort_receiver: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let streaming_key = &streaming_key;
    log::debug!("Initializing stream seeker daemon for {:?}", streaming_key);
    let kad_key: Key =
        StreamingRecord::streaming_kad_key(&streaming_key.video_key, &streaming_key.channel_key)
            .into();

    let record;
    {
        loop {
            tokio::task::yield_now().await;
            if abort_receiver.try_recv().is_ok() {
                log::info!("Aborted for {streaming_key:?}, exiting...");
                return Ok(());
            }
            log::debug!("Trying to get record {:?}", streaming_key);
            match network.get_record(kad_key.clone()).await {
                Ok(KademliaRecord::MediaStreaming(r)) => {
                    record = r;
                    break;
                }
                Ok(other) => {
                    anyhow::bail!(
                        "Searching for record {:?} returned error wrong record {:?}",
                        &streaming_key,
                        other
                    );
                }
                Err(e) => {
                    log::info!(
                        "Searching for record {:?} returned error {}, sleeping a little",
                        &streaming_key,
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            };
        }
    }

    let stream_publisher_client = {
        let client = shared_state
            .active_streams
            .lock()
            .await
            .get(streaming_key)
            .cloned();
        match client {
            Some(c) => {
                log::debug!("Found stream daemon for {:?}", streaming_key);
                if initializer.send(Ok(c.clone())).is_err() {
                    anyhow::bail!("Initializer died");
                }
                c
            }
            None => stream_publisher::launch_daemon(
                streaming_key.clone(),
                shared_state.clone(),
                network.clone(),
                initializer,
                record.clone(),
            )
            .await
            .context("failed to launch daemon")?,
        }
    };

    let (_seeker_handle, seeker_client) = peer_seeker::launch_daemon(
        identity,
        shared_state.clone(),
        network.clone(),
        streaming_key.clone(),
    );
    let mut seek_type = StreamSeekType::Reset;
    let mut empty_data_count = 0;
    let mut consecutive_errors = 0;
    loop {
        tokio::task::yield_now().await;
        if abort_receiver.try_recv().is_ok() {
            log::info!("Aborted for {streaming_key:?}, exiting...");
            return Ok(());
        }
        let peers = seeker_client.get_peers().await?;
        if peers.is_empty() {
            log::debug!("No peers found for {streaming_key:?}, sleeping a little...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            let future_requests = peers.into_iter().map(|peer| {
                let request = StreamingRequest {
                    streaming_key: streaming_key.clone(),
                    seek_type,
                };
                let network = network.clone();
                log::debug!("Calling {streaming_key:?} on {peer} with {seek_type:?}");
                Box::pin(async move {
                    match network.request_streaming_data(request, peer).await {
                        Ok(r) => Ok((peer, r)),
                        Err(e) => Err((peer, e)),
                    }
                })
            });

            let request_result = match futures::future::select_ok(future_requests).await {
                Ok((result, _vec)) => result,
                Err((_peer, e)) => return Err(e),
            };

            match request_result {
                (peer, Ok(Ok(StreamingResponse::Data(responses)))) if !responses.is_empty() => {
                    let responses_len = responses.len();
                    let valid_responses_len = utils::measure_noop("r.verify", || {
                        responses.par_iter().filter(|r| r.verify(&record)).count()
                    });
                    if responses_len != valid_responses_len {
                        log::warn!(
                            "Received {} responses but only {} are valid, skipping peer for now",
                            responses_len,
                            valid_responses_len
                        );
                        seeker_client.ban_on_recoverable_error(peer).await?;
                    }
                    // reset counters
                    empty_data_count = 0;
                    consecutive_errors = 0;
                    log::info!(
                        "Found {} responses for {:?} from {}",
                        responses_len,
                        streaming_key,
                        {
                            if streaming_key.channel_key == peer {
                                "original peer".to_string()
                            } else {
                                format!("{:?}", peer)
                            }
                        }
                    );
                    let new_source_offset = responses
                        .iter()
                        .map(|r| r.streaming_data.source_offset)
                        .max()
                        .context("empty list")?;

                    if let StreamSeekType::Offset(existing_source_offset) = seek_type {
                        if existing_source_offset >= new_source_offset {
                            log::warn!("Received offset {:?} which isn't above current {:?} from peer {}, blacklisting and skipping...", new_source_offset, existing_source_offset, peer);
                            seeker_client.ban(peer).await?;
                        }
                    }
                    seek_type = StreamSeekType::Offset(new_source_offset);
                    stream_publisher_client
                        .feed_data(responses.into_iter().collect())
                        .await
                        .context("daemon alive!?")?; // TODO: check what to do
                }
                (peer, Ok(Ok(StreamingResponse::Data(_)))) => {
                    const MAX_EMPTY_DATA: usize = 5;
                    empty_data_count += 1;
                    if empty_data_count >= MAX_EMPTY_DATA {
                        log::warn!(
                            "{empty_data_count} empty responses for {streaming_key:?}, sleeping a little",
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        log::info!("Received empty response from {}", peer);
                    }
                }
                (peer, Ok(Ok(StreamingResponse::MaxUploadRateReached))) => {
                    log::warn!(
                        "Received StreamingResponse::MaxUploadRateReached response from {peer}"
                    );
                    seeker_client.ban_on_max_upload(peer).await?;
                }
                (peer, Ok(Ok(StreamingResponse::TooMuchFlood))) => {
                    log::warn!("Received StreamingResponse::TooMuchFlood response from {peer}");
                    seeker_client.ban_on_flood(peer).await?;
                }
                (peer, Ok(Err(e))) => {
                    log::warn!("Received error {e} from {peer}");
                    seeker_client.ban_on_recoverable_error(peer).await?;
                    consecutive_errors += 1;
                }
                (peer, Err(OutboundFailure::UnsupportedProtocols)) => {
                    log::debug!("Received OutboundFailure::UnsupportedProtocols from {peer}");
                    seeker_client.ban(peer).await?;
                    consecutive_errors += 1;
                }
                (
                    peer,
                    Err(
                        e @ (OutboundFailure::ConnectionClosed
                        | OutboundFailure::DialFailure
                        | OutboundFailure::Timeout),
                    ),
                ) => {
                    log::info!("Received {e:?} from {peer}");
                    seeker_client.ban_on_recoverable_error(peer).await?;
                    consecutive_errors += 1;
                }
            }
        }
        {
            const MAX_CONSECUTIVE_ERRORS: usize = 3;
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                log::info!("Sleeping a little because too many errors are being received");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        {
            const MAX_INACTIVITY_TIME: Duration = Duration::from_secs(5 * 60);
            let last_sent = stream_publisher_client
                .get_last_sent()
                .await
                .context("failed to receive")?;
            if instant::Instant::now().duration_since(last_sent) > MAX_INACTIVITY_TIME {
                log::info!("Exiting {streaming_key:?} because of inactivity");
                stream_publisher_client
                    .die()
                    .await
                    .context("failed to receive")?;
                return Ok(());
            }
        }
    }
}
