// SPDX-License-Identifier: AGPL-3.0-only

use std::time::{Duration, SystemTime};

use libp2p::kad::record::Key;
use libp2p::request_response::OutboundFailure;
use rayon::prelude::*;
use tokio::sync::oneshot;

use super::rtmp_publisher::RtmpPublisherClient;
use crate::daemons::cm::rtmp::{peer_seeker, rtmp_publisher};
use crate::daemons::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::{KademliaRecord, RtmpStreamingRecord};
use crate::protocols::rtmp_streaming::{
    RTMPDataSeekType, RTMPStreamingRequest, RtmpStreamingKey, RtmpStreamingResponse,
};
use crate::protocols::NodeIdentity;
use crate::utils;


pub async fn launch_daemon(
    identity: NodeIdentity,
    streaming_key: RtmpStreamingKey,
    sender: oneshot::Sender<anyhow::Result<RtmpPublisherClient>>,
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
            let task = utils::spawn_and_log_error({
                let shared_state = shared_state.clone();
                async move {
                    let r = _stream_seeker_daemon(
                        identity,
                        streaming_key.clone(),
                        shared_state.clone(),
                        network,
                        sender,
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
            entry.insert(task);
            tokio::task::yield_now().await;
        }
    };
    Ok(())
}

async fn _stream_seeker_daemon(
    identity: NodeIdentity,
    streaming_key: RtmpStreamingKey,
    shared_state: SharedGlobalState,
    mut network: NetworkBackendClient,
    initializer: oneshot::Sender<anyhow::Result<RtmpPublisherClient>>,
) -> anyhow::Result<()> {
    let streaming_key = &streaming_key;
    log::debug!("Initializing stream seeker daemon for {:?}", streaming_key);
    let kad_key: Key = RtmpStreamingRecord::rtmp_streaming_kad_key(
        &streaming_key.app_name,
        &streaming_key.stream_key,
    )
    .into();

    let record;
    {
        loop {
            log::debug!("Trying to get record {:?}", streaming_key);
            match network.get_record(kad_key.clone()).await {
                Ok(KademliaRecord::Rtmp(r)) => {
                    record = r;
                    break;
                }
                Ok(other) => {
                    return Err(anyhow::anyhow!(
                        "Searching for record {:?} returned error wrong record {:?}",
                        &streaming_key,
                        other
                    ));
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

    let rtmp_publisher_client = {
        let client = shared_state
            .active_streams
            .lock()
            .await
            .get(streaming_key)
            .cloned();
        match client {
            Some(c) => {
                log::debug!("Found rtmp daemon for {:?}", streaming_key);
                if initializer.send(Ok(c.clone())).is_err() {
                    anyhow::bail!("Initializer died");
                }
                c
            }
            None => rtmp_publisher::launch_daemon(
                streaming_key.clone(),
                shared_state.clone(),
                network.clone(),
                initializer,
                record.clone(),
            )
            .await
            .expect("to launch daemon"),
        }
    };

    let (_seeker_handle, seeker_client) = peer_seeker::launch_daemon(
        identity,
        shared_state.clone(),
        network.clone(),
        streaming_key.clone(),
    );
    let mut seek_type = RTMPDataSeekType::Reset;
    let mut empty_data_count = 0;
    let mut consecutive_errors = 0;
    loop {
        let peers = seeker_client.get_peers().await;
        if peers.is_empty() {
            log::debug!("No peers found for {streaming_key:?}, sleeping a little...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        for peer in peers {
            let request = RTMPStreamingRequest {
                streaming_key: streaming_key.clone(),
                seek_type,
            };
            log::debug!("Calling {streaming_key:?} on {peer} with {seek_type:?}");
            match network.request_rtmp_streaming_data(request, peer).await? {
                Ok(Ok(RtmpStreamingResponse::Data(responses))) if !responses.is_empty() => {
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
                        seeker_client.ban_on_recoverable_error(peer).await;
                    }
                    // reset counters
                    empty_data_count = 0;
                    consecutive_errors = 0;
                    log::info!(
                        "Found {} responses for {:?} from {}",
                        responses_len,
                        streaming_key,
                        {
                            if streaming_key.stream_key == peer {
                                "original peer".to_string()
                            } else {
                                format!("{:?}", peer)
                            }
                        }
                    );
                    let new_source_offset = responses
                        .iter()
                        .map(|r| r.rtmp_data.source_offset)
                        .max()
                        .expect("non empty list");

                    if let RTMPDataSeekType::Offset(existing_source_offset) = seek_type {
                        if existing_source_offset >= new_source_offset {
                            log::warn!("Received offset {:?} which isn't above current {:?} from peer {}, blacklisting and skipping...", new_source_offset, existing_source_offset, peer);
                            seeker_client.ban(peer).await;
                        }
                    }
                    seek_type = RTMPDataSeekType::Offset(new_source_offset);
                    rtmp_publisher_client
                        .feed_data(responses.into_iter().collect())
                        .await
                        .expect("daemon alive!?"); // TODO: check what to do
                }
                Ok(Ok(RtmpStreamingResponse::Data(_))) => {
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
                Ok(Ok(RtmpStreamingResponse::MaxUploadRateReached)) => {
                    log::warn!(
                        "Received RtmpStreamingResponse::MaxUploadRateReached response from {peer}"
                    );
                    seeker_client.ban_on_max_upload(peer).await;
                }
                Ok(Ok(RtmpStreamingResponse::TooMuchFlood)) => {
                    log::warn!("Received RtmpStreamingResponse::TooMuchFlood response from {peer}");
                    seeker_client.ban_on_flood(peer).await;
                }
                Ok(Err(e)) => {
                    log::warn!("Received error {e} from {peer}");
                    seeker_client.ban_on_recoverable_error(peer).await;
                    consecutive_errors += 1;
                }
                Err(OutboundFailure::UnsupportedProtocols) => {
                    log::debug!("Received OutboundFailure::UnsupportedProtocols from {peer}");
                    seeker_client.ban(peer).await;
                    consecutive_errors += 1;
                }
                Err(
                    e @ (OutboundFailure::ConnectionClosed
                    | OutboundFailure::DialFailure
                    | OutboundFailure::Timeout),
                ) => {
                    log::info!("Received {e:?} from {peer}");
                    seeker_client.ban_on_recoverable_error(peer).await;
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
            let last_sent = rtmp_publisher_client
                .get_last_sent()
                .await
                .expect("to receive");
            if SystemTime::now().duration_since(last_sent).unwrap() > MAX_INACTIVITY_TIME {
                log::info!("Exiting {streaming_key:?} because of inactivity");
                rtmp_publisher_client.die().await.expect("to receive");
                return Ok(());
            }
        }
    }
}
