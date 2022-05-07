use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use anyhow::Context;
use galtcore::cm::{self, SharedGlobalState};
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::delegated_streaming::{
    DelegatedStreamingRequest, PlayStreamNewTrackInfo, WebRtcStream, WebRtcTrack,
};
use galtcore::protocols::media_streaming::{
    IntegerStreamTrack, StreamOffset, StreamSeekType, StreamingDataType, StreamingKey,
    StreamingResponse,
};
use galtcore::protocols::NodeIdentity;
use galtcore::utils;
use galtcore::utils::ArcMutex;
use instant::Duration;
use libp2p::PeerId;
use tokio::sync::{oneshot, Mutex};
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use webrtc::track::track_local::TrackLocalWriter;

use super::PeerState;

pub(super) struct DelegatedStreamingPlayingState {
    pub(super) tracks: ArcMutex<HashMap<IntegerStreamTrack, Arc<TrackLocalStaticRTP>>>,
}

pub(super) async fn play(
    peer: PeerId,
    streaming_key: StreamingKey,
    peer_state: PeerState,
    identity: NodeIdentity,
    shared_global_state: SharedGlobalState,
    mut network: NetworkBackendClient,
) -> anyhow::Result<DelegatedStreamingPlayingState> {
    let (sender, receiver) = oneshot::channel();
    cm::modules::streaming::play(
        identity,
        &shared_global_state,
        &mut network,
        cm::modules::streaming::PlayStreamInfo {
            streaming_key: streaming_key.clone(),
            sender,
        },
    )
    .await?;
    let publisher = receiver.await??;

    let mut seek_type = StreamSeekType::Reset;
    let mut stream_offset = StreamOffset {
        timestamp_reference: 0,
        sequence_id: 0,
    };


    let tracks: ArcMutex<HashMap<IntegerStreamTrack, Arc<TrackLocalStaticRTP>>> =
        Arc::new(Mutex::new(HashMap::new()));

    utils::spawn_and_log_error({
        let tracks = Arc::clone(&tracks);
        async move {
            let mut h264_processor = crate::media::RtmpH264ToPackets::new(90000, 96);
            loop {
                tokio::task::yield_now().await;
                match publisher.get_data(peer, seek_type).await {
                    Ok(Ok(StreamingResponse::Data(data_vector))) if !data_vector.is_empty() => {
                        log::debug!(
                            "play_stream: reading {} packets for {streaming_key} and {peer} {seek_type:?}",
                            data_vector.len()
                        );
                        {
                            // it's okay to keep this lock for a while because no other task is supposed to be actively reading this data structure
                            let mut local_tracks = tracks.lock().await;
                            for r in data_vector {
                                stream_offset =
                                    std::cmp::max(r.streaming_data.source_offset, stream_offset);
                                let local_track = match local_tracks
                                    .entry(r.streaming_data.stream_track.clone())
                                {
                                    hash_map::Entry::Occupied(mut e) => e.get_mut().to_owned(),
                                    hash_map::Entry::Vacant(e) => match r.streaming_data.metadata {
                                        Some(metadata) => {
                                            let webrtc_track = WebRtcTrack {
                                                track_id: r
                                                    .streaming_data
                                                    .stream_track
                                                    .track_id
                                                    .to_string(),
                                                stream_id: WebRtcStream(
                                                    r.streaming_data
                                                        .stream_track
                                                        .stream_id
                                                        .to_string(),
                                                ),
                                            };
                                            let local_track = Arc::new(TrackLocalStaticRTP::new(
                                                super::to_codec_capability(metadata.capability),
                                                webrtc_track.track_id.clone(),
                                                webrtc_track.stream_id.0.clone(),
                                            ));
                                            log::info!("Initializing {local_track:?}");
                                            e.insert(Arc::clone(&local_track));
                                            network
                                                .request_delegated_streaming(
                                                    DelegatedStreamingRequest::PlayStreamNewTrack(
                                                        PlayStreamNewTrackInfo {
                                                            streaming_key: streaming_key.clone(),
                                                            tracks: vec![webrtc_track],
                                                        },
                                                    ),
                                                    peer,
                                                )
                                                .await
                                                .context("request_delegated_streaming sending PlayStreamNewTrack first error")?
                                                .context("request_delegated_streaming sending PlayStreamNewTrack second error")?
                                                .map_err(|e| anyhow::anyhow!("request_delegated_streaming sending PlayStreamNewTrack last error: {e}"))?;
                                            peer_state
                                                .connection
                                                .add_track(Arc::clone(&local_track)
                                                    as Arc<
                                                        dyn webrtc::track::track_local::TrackLocal
                                                            + Send
                                                            + Sync,
                                                    >)
                                                .await?;
                                            local_track
                                        }
                                        None => {
                                            todo!()
                                        }
                                    },
                                };
                                let packets = {
                                    match r.streaming_data.data_type {
                                        StreamingDataType::RtmpAudio(m)
                                        | StreamingDataType::RtmpVideo(m) => {
                                            match h264_processor
                                                .process(&r.streaming_data.data, m.timestamp)
                                            {
                                                Ok(packets) => packets,
                                                Err(e) => {
                                                    log::warn!(
                                                        "Skipping packet processing from rtmp for {peer} {streaming_key}: {e:?}"
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                        StreamingDataType::WebRtcRtpPacket => {
                                            match webrtc_util::Unmarshal::unmarshal(
                                                &mut r.streaming_data.data.as_slice(),
                                            ) {
                                                Ok(packet) => vec![packet],
                                                Err(e) => {
                                                    log::warn!(
                                                        "Skipping packet serialization for {peer} {streaming_key}: {e:?}"
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                };
                                for packet in &packets {
                                    match local_track.write_rtp(packet).await {
                                        Ok(_) => {
                                            log::debug!(
                                                "Written packet to {peer} for {streaming_key}: {:?}",
                                                packet.header
                                            )
                                        }
                                        Err(e) => log::warn!(
                                            "Skipping packet write for {peer} {streaming_key}: {e:?}"
                                        ),
                                    };
                                }
                            }
                            seek_type = StreamSeekType::Offset(stream_offset);
                        }
                    }
                    Ok(Ok(StreamingResponse::Data(_))) => {
                        log::debug!("Received empty data, sleeping a little");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Ok(Ok(StreamingResponse::MaxUploadRateReached)) => {
                        log::error!("Received StreamingResponse::MaxUploadRateReached");
                    }
                    Ok(Ok(StreamingResponse::TooMuchFlood)) => {
                        log::error!("Received StreamingResponse::TooMuchFlood");
                    }
                    Ok(Err(e)) => anyhow::bail!("{}", e),
                    Err(e) => return Err(e),
                }
            }
        }
    });
    Ok(DelegatedStreamingPlayingState { tracks })
}
