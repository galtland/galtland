use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use galtcore::daemons::cm::{self, ClientCommand};
use galtcore::protocols::delegated_streaming::{self, WebRtcTrack};
use galtcore::protocols::kademlia_record::StreamingRecord;
use galtcore::protocols::media_streaming::{
    MyRTCRtpCodecCapability, SignedStreamingData, StreamMetadata, StreamOffset, StreamTrack,
    StreamingData, StreamingDataType,
};
use galtcore::protocols::NodeIdentity;
use galtcore::tokio_stream::StreamExt;
use galtcore::utils;
use libp2p::futures::stream::FuturesUnordered;
use tokio::sync::{mpsc, oneshot};
use webrtc::track::track_remote::TrackRemote;


pub(super) enum DelegatedStreamingPublishingState {
    NotPublishing,
    GatheringInfo {
        pending_remote_tracks: HashMap<WebRtcTrack, Arc<TrackRemote>>,
        pending_publishing_info: Option<delegated_streaming::PublishStreamInfo>,
    },
    Publishing {
        info: delegated_streaming::PublishStreamInfo,
        tracks: HashMap<StreamTrack, (MyRTCRtpCodecCapability, Arc<TrackRemote>)>,
    },
}

struct PacketReadContext {
    metadata: StreamMetadata,
    stream_track: StreamTrack,
    webrtc_track: WebRtcTrack,
    remote_track: Arc<TrackRemote>,
}


type RemoteTrackReadResult =
    Result<(PacketReadContext, webrtc::rtp::packet::Packet), (PacketReadContext, webrtc::Error)>;

async fn get_next_read(context: PacketReadContext) -> RemoteTrackReadResult {
    match context.remote_track.read_rtp().await {
        Ok((packet, _attributes)) => Ok((context, packet)),
        Err(e) => Err((context, e)),
    }
}

pub(super) async fn publish(
    info: delegated_streaming::PublishStreamInfo,
    remote_tracks: &HashMap<WebRtcTrack, Arc<TrackRemote>>,
    commands: &mpsc::Sender<ClientCommand>,
    identity: NodeIdentity,
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
    commands
        .send(ClientCommand::PublishStream(
            cm::streaming::handlers::PublishStreamInfo {
                record: record.clone(),
                sender,
            },
        ))
        .await
        .map_err(utils::send_error)?;

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
        let stream_track = StreamTrack {
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
                    Ok((context, packet)) => {
                        let data = {
                            let len = webrtc_util::marshal::MarshalSize::marshal_size(&packet);
                            let mut b = vec![0; len];
                            webrtc_util::marshal::Marshal::marshal_to(&packet, &mut b)?;
                            b
                        };
                        log::trace!(
                            "Received packet for {:?} {:?} with {} bytes",
                            context.webrtc_track,
                            streaming_key,
                            data.len()
                        );
                        let streaming_data = vec![SignedStreamingData::from(
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
            }
            log::info!("Exiting loop for {streaming_key}");

            Ok(())
        }
    });
    let new_state = DelegatedStreamingPublishingState::Publishing { info, tracks };
    Ok(new_state)
}
