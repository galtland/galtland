// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

use libp2p::request_response::ResponseChannel;
use libp2p::PeerId;
use log::debug;
use tokio::sync::oneshot;

use super::stream_publisher::{self, StreamPublisherClient};
use crate::configuration::Configuration;
use crate::daemons::cm::peer_control::FloodControlResult;
use crate::daemons::cm::streaming::stream_seeker;
use crate::daemons::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::StreamingRecord;
use crate::protocols::media_streaming::{
    StreamingKey, StreamingRequest, StreamingResponse, WrappedStreamingResponseResult,
};
use crate::protocols::payment_info::{PaymentInfoRequest, PaymentInfoResponseResult};
use crate::protocols::NodeIdentity;

pub struct PublishStreamInfo {
    pub record: StreamingRecord,
    pub sender: oneshot::Sender<anyhow::Result<StreamPublisherClient>>,
}

#[derive(Debug)]
pub struct RespondStreamingInfo {
    pub peer: PeerId,
    pub request: StreamingRequest,
    pub channel: ResponseChannel<WrappedStreamingResponseResult>,
}

#[derive(Debug)]
pub struct RespondPaymentInfo {
    pub peer: PeerId,
    pub request: PaymentInfoRequest,
    pub channel: ResponseChannel<PaymentInfoResponseResult>,
}

pub struct PlayStreamInfo {
    pub streaming_key: StreamingKey,
    pub sender: oneshot::Sender<anyhow::Result<StreamPublisherClient>>,
}

pub(crate) async fn publish(
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    info: PublishStreamInfo,
) -> anyhow::Result<()> {
    let PublishStreamInfo { sender, record } = info;
    let streaming_key = &record.streaming_key;
    debug!("publish {:?}", streaming_key);
    shared_state
        .stream_seeker
        .lock()
        .await
        .remove(streaming_key)
        .into_iter()
        .for_each(|s| {
            log::info!(
                "Aborting stream seeker for {:?} to be able to publish",
                streaming_key
            );
            if s.send(()).is_err() {
                log::warn!(
                    "While aborting: stream seeker for {streaming_key:?} seems to be already dead"
                );
            }
        });
    let daemon_sender = shared_state
        .active_streams
        .lock()
        .await
        .get(streaming_key)
        .cloned();
    match daemon_sender {
        Some(daemon_sender) => {
            if sender.send(Ok(daemon_sender)).is_err() {
                anyhow::bail!("receiver died");
            }
        }
        None => {
            stream_publisher::launch_daemon(
                streaming_key.clone(),
                shared_state.clone(),
                network.clone(),
                sender,
                record,
            )
            .await
            .expect("to launch daemon");
        }
    };
    Ok(())
}

pub(crate) async fn respond(
    opt: Configuration,
    identity: NodeIdentity,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    info: RespondStreamingInfo,
) -> anyhow::Result<()> {
    let RespondStreamingInfo {
        peer,
        request,
        channel,
    } = info;
    match shared_state
        .peer_control
        .lock()
        .await
        .flood_control(&peer, request.clone())
    {
        FloodControlResult::Good => {
            debug!("respond is good {} {:?}", peer, request);
        }
        other @ (FloodControlResult::Blacklisted | FloodControlResult::StillBlacklisted) => {
            debug!("respond is bad {} {:?}: {:?}", peer, request, other);
            network
                .respond_streaming_data(peer, Ok(StreamingResponse::TooMuchFlood), channel)
                .await?;
            return Ok(());
        }
    };
    let daemon_sender = shared_state
        .active_streams
        .lock()
        .await
        .get(&request.streaming_key)
        .cloned();
    let stream_publisher_client = match daemon_sender {
        Some(d) => d,
        None => {
            let current_bytes_per_second =
                shared_state.sent_stats.lock().await.get_bytes_per_second();
            let max_bytes_per_second = opt
                .max_bytes_per_second_upload_stream
                .unwrap_or(byte_unit::Byte::from_bytes(u128::MAX))
                .get_bytes();
            let has_room = current_bytes_per_second < max_bytes_per_second;
            if has_room {
                log::info!(
                    "Responding to a media stream {:?} that we (probably) didn't provide for because we have room for more uploads: {} < {}",
                    request.streaming_key,
                    current_bytes_per_second,
                    max_bytes_per_second
                );
                let (sender, receiver) = oneshot::channel();
                stream_seeker::launch_daemon(
                    identity,
                    request.streaming_key,
                    sender,
                    shared_state.clone(),
                    network.clone(),
                )
                .await?;
                tokio::task::yield_now().await;
                receiver.await??
            } else {
                log::info!(
                    "Rejecting media stream {:?} that we (probably) didn't provide for because we don't have room for more uploads: {} >= {}",
                    request.streaming_key,
                    current_bytes_per_second,
                    max_bytes_per_second
                );
                network
                    .respond_streaming_data(
                        peer,
                        Ok(StreamingResponse::MaxUploadRateReached),
                        channel,
                    )
                    .await?;
                return Ok(());
            }
        }
    };

    let mut empty_responses_count = 0;
    loop {
        let seek_type = request.seek_type;
        tokio::task::yield_now().await;
        match stream_publisher_client.get_data(peer, seek_type).await {
            Ok(Ok(StreamingResponse::Data(r))) if r.is_empty() => {
                empty_responses_count += 1;
                if empty_responses_count >= 10 {
                    log::info!("Answering with empty response because we can't await anymore, request get data: {peer} {seek_type:?}");
                    network
                        .respond_streaming_data(peer, Ok(StreamingResponse::Data(r)), channel)
                        .await?;
                    break;
                } else {
                    // FIXME: perhaps work with timeouts so we can always receive something
                    log::debug!("Got empty response, will sleep a little");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Ok(Ok(r)) => {
                network.respond_streaming_data(peer, Ok(r), channel).await?;
                break;
            }
            Ok(Err(e)) => {
                network
                    .respond_streaming_data(peer, Err(e.to_string()), channel)
                    .await?;
                break;
            }
            Err(e) => {
                network
                    .respond_streaming_data(peer, Err(e.to_string()), channel)
                    .await?;
                break;
            }
        }
    }
    Ok(())
}

pub(crate) async fn play(
    identity: NodeIdentity,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    info: PlayStreamInfo,
) -> anyhow::Result<()> {
    let PlayStreamInfo {
        streaming_key,
        sender,
    } = info;
    debug!("play {:?}", streaming_key);
    let stream_publisher_client = shared_state
        .active_streams
        .lock()
        .await
        .get(&streaming_key)
        .cloned();
    match stream_publisher_client {
        Some(stream_publisher_client) => {
            if sender.send(Ok(stream_publisher_client)).is_err() {
                anyhow::bail!("receiver died");
            }
            Ok(())
        }
        None => {
            stream_seeker::launch_daemon(identity, streaming_key, sender, shared_state, network)
                .await
        }
    }
}
