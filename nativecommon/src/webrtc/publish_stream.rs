use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use galtcore::cm::{self, SharedGlobalState};
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::delegated_streaming::{self, WebRtcTrack};
use galtcore::protocols::kademlia_record::StreamingRecord;
use galtcore::protocols::media_streaming::{
    IntegerStreamTrack, MyRTCRtpCodecCapability, StreamMetadata, StreamOffset, StreamingData,
    StreamingDataType, VerifiedSignedStreamingData,
};
use galtcore::protocols::NodeIdentity;
use galtcore::tokio_stream::StreamExt;
use galtcore::utils;
use instant::Duration;
use libp2p::futures::stream::FuturesUnordered;
use tokio::sync::oneshot;
use webrtc::track::track_remote::TrackRemote;


pub(super) enum DelegatedStreamingPublishingState {
    NotPublishing,
    GatheringInfo {
        pending_remote_tracks: HashMap<WebRtcTrack, Arc<TrackRemote>>,
        pending_publishing_info: Option<delegated_streaming::PublishStreamInfo>,
    },
    Publishing {
        info: delegated_streaming::PublishStreamInfo,
        tracks: HashMap<IntegerStreamTrack, (MyRTCRtpCodecCapability, Arc<TrackRemote>)>,
    },
}

struct PacketReadContext {
    metadata: StreamMetadata,
    stream_track: IntegerStreamTrack,
    webrtc_track: WebRtcTrack,
    remote_track: Arc<TrackRemote>,
}

const WARN_READ_DURATION: Duration = Duration::from_millis(100);

type RemoteTrackReadResult =
    Result<(PacketReadContext, Vec<u8>), (PacketReadContext, webrtc::Error)>;

async fn get_next_read(context: PacketReadContext) -> RemoteTrackReadResult {
    let mut b = vec![0u8; 4096];
    let now = instant::Instant::now();
    let result = match context.remote_track.read(&mut b).await {
        Ok((packet_size, _attributes)) => {
            b.truncate(packet_size);
            Ok((context, b))
        }
        Err(e) => Err((context, e)),
    };
    let took = now.elapsed();
    if took > WARN_READ_DURATION {
        log::warn!("context.remote_track.read took: {:?}", took);
    } else {
        log::trace!("context.remote_track.read took: {:?}", took);
    }
    result
}

pub(super) async fn publish(
    info: delegated_streaming::PublishStreamInfo,
    remote_tracks: &HashMap<WebRtcTrack, Arc<TrackRemote>>,
    identity: NodeIdentity,
    shared_global_state: &SharedGlobalState,
    network: &mut NetworkBackendClient,
) -> anyhow::Result<DelegatedStreamingPublishingState> {
    let streaming_key = info.streaming_key.clone();

    log::info!(
        "Publishing {streaming_key} using {} tracks",
        info.tracks.len()
    );

    let record = StreamingRecord::new(
        &identity.keypair,
        &streaming_key,
        instant::SystemTime::now().into(),
    )?;
    let (sender, receiver) = oneshot::channel();
    cm::modules::streaming::publish(
        shared_global_state,
        network,
        cm::modules::streaming::PublishStreamInfo {
            record: record.clone(),
            sender,
        },
    )
    .await?;

    let publisher = receiver
        .await
        .context("first publish receive error")?
        .context("second publish receive error")?;
    let mut tracks = HashMap::new();
    let mut futures = FuturesUnordered::new();
    let mut stream_id_map = HashMap::new();

    for (i, (webrtc_track, remote_track)) in remote_tracks.iter().enumerate() {
        let i = i as u8;
        let stream_id = match stream_id_map.entry(webrtc_track.stream_id.clone()) {
            hash_map::Entry::Occupied(e) => *e.get(),
            hash_map::Entry::Vacant(e) => {
                e.insert(i);
                i
            }
        };
        let track_id = i;
        let stream_track = IntegerStreamTrack {
            stream_id,
            track_id,
        };
        let capability = super::from_codec_capability(remote_track.codec().await.capability);
        tracks.insert(
            stream_track.clone(),
            (capability.clone(), Arc::clone(remote_track)),
        );
        let metadata = StreamMetadata { capability };
        let context = PacketReadContext {
            metadata,
            remote_track: Arc::clone(remote_track),
            stream_track,
            webrtc_track: webrtc_track.clone(),
        };
        futures.push(get_next_read(context));
    }

    log::info!(
        "Created {} track readers for {streaming_key}",
        futures.len()
    );

    utils::spawn_and_log_error({
        let streaming_key = info.streaming_key.clone();
        async move {
            let timestamp_reference = chrono::offset::Utc::now().timestamp() as u32;
            let mut sequence_id = 0;
            while let Some(result) = futures.next().await {
                match result {
                    Ok((context, data)) => {
                        log::trace!(
                            "Received packet for {:?} {:?} with {} bytes",
                            context.webrtc_track,
                            streaming_key,
                            data.len()
                        );
                        let streaming_data = vec![VerifiedSignedStreamingData::from(
                            StreamingData {
                                data,
                                source_offset: StreamOffset {
                                    sequence_id,
                                    timestamp_reference,
                                },
                                data_type: StreamingDataType::WebRtcRtpPacket,
                                metadata: Some(context.metadata.clone()),
                                stream_track: context.stream_track.clone(),
                            },
                            &identity.keypair,
                            &record,
                        )?];
                        publisher.feed_data(streaming_data).await?;
                        sequence_id += 1;
                        futures.push(get_next_read(context));
                    }
                    Err((context, e)) => {
                        log::warn!(
                            "Error reading remote track {:?}: {e:?}",
                            context.remote_track
                        );
                        futures.push(get_next_read(context));
                    }
                }
                tokio::task::yield_now().await;
            }
            log::info!("Exiting loop for {streaming_key}");
            Ok(())
        }
    });
    let new_state = DelegatedStreamingPublishingState::Publishing { info, tracks };
    Ok(new_state)
}
