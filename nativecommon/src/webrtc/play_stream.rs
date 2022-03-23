use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use anyhow::Context;
use galtcore::daemons::cm::{self, ClientCommand};
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::delegated_streaming::{
    DelegatedStreamingRequest, PlayStreamNewTrackInfo, WebRtcStream, WebRtcTrack,
};
use galtcore::protocols::media_streaming::{
    StreamOffset, StreamSeekType, StreamTrack, StreamingDataType, StreamingKey, StreamingResponse,
};
use galtcore::utils;
use galtcore::utils::ArcMutex;
use instant::Duration;
use itertools::Itertools;
use libp2p::PeerId;
use tokio::sync::{mpsc, oneshot, Mutex};
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use webrtc::track::track_local::TrackLocalWriter;

use super::PeerState;

pub(super) struct DelegatedStreamingPlayingState {
    pub(super) tracks: ArcMutex<HashMap<StreamTrack, Arc<TrackLocalStaticRTP>>>,
}

pub(super) async fn play(
    peer: PeerId,
    streaming_key: StreamingKey,
    peer_state: PeerState,
    commands: mpsc::Sender<ClientCommand>,
    network: NetworkBackendClient,
) -> anyhow::Result<DelegatedStreamingPlayingState> {
    let (sender, receiver) = oneshot::channel();
    commands
        .send(ClientCommand::PlayStream(
            cm::streaming::handlers::PlayStreamInfo {
                streaming_key: streaming_key.clone(),
                sender,
            },
        ))
        .await
        .map_err(utils::send_error)?;
    let publisher = receiver.await??;

    let mut seek_type = StreamSeekType::Reset;
    let mut stream_offset = StreamOffset {
        timestamp_reference: 0,
        sequence_id: 0,
    };

    let tracks: ArcMutex<HashMap<StreamTrack, Arc<TrackLocalStaticRTP>>> =
        Arc::new(Mutex::new(HashMap::new()));

    utils::spawn_and_log_error({
        let tracks = Arc::clone(&tracks);
        async move {
            loop {
                match publisher.get_data(peer, seek_type).await {
                    Ok(Ok(StreamingResponse::Data(data_vector))) if !data_vector.is_empty() => {
                        log::debug!(
                            "play_stream: reading {} packets for {streaming_key} and {peer} {seek_type:?}",
                            data_vector.len()
                        );
                        let data_vector = data_vector
                            .into_iter()
                            .map(|r| {
                                stream_offset =
                                    std::cmp::max(r.streaming_data.source_offset, stream_offset);
                                match r.streaming_data.data_type {
                                    StreamingDataType::RtmpAudio | StreamingDataType::RtmpVideo => {
                                        todo!()
                                    }
                                    StreamingDataType::WebRtcRtpPacket => r.streaming_data,
                                }
                            })
                            .collect_vec();

                        seek_type = StreamSeekType::Offset(stream_offset);

                        {
                            // it's okay to keep this lock for a while because no other task is supposed to be actively reading this data structure
                            let mut local_tracks = tracks.lock().await;
                            for streaming_data in data_vector {
                                let local_track = match local_tracks
                                    .entry(streaming_data.stream_track.clone())
                                {
                                    hash_map::Entry::Occupied(mut e) => e.get_mut().to_owned(),
                                    hash_map::Entry::Vacant(e) => match streaming_data.metadata {
                                        Some(metadata) => {
                                            let webrtc_track = WebRtcTrack {
                                                track_id: streaming_data
                                                    .stream_track
                                                    .track_id
                                                    .to_string(),
                                                stream_id: WebRtcStream(
                                                    streaming_data
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
                                local_track.write(&streaming_data.data).await?;
                            }
                        }
                    }
                    Ok(Ok(StreamingResponse::Data(_))) => {
                        log::info!("Received empty data, sleeping a little");
                        tokio::time::sleep(Duration::from_millis(500)).await;
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
