use std::sync::Arc;

use anyhow::{Context, Result};
use galtcore::rmp_serde;
use tokio::sync::mpsc;
use tokio::time::Duration;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_candidate::RTCIceCandidate;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::offer_answer_options::RTCOfferOptions;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
use webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver;
use webrtc::track::track_remote::TrackRemote;


pub async fn prepare_connection() -> Result<(
    Arc<RTCPeerConnection>,
    mpsc::UnboundedReceiver<Arc<TrackRemote>>,
    mpsc::UnboundedReceiver<Option<RTCIceCandidate>>,
    mpsc::UnboundedReceiver<()>,
)> {
    let mut m = MediaEngine::default();

    m.register_default_codecs()?;

    // Create a InterceptorRegistry. This is the user configurable RTP/RTCP Pipeline.
    // This provides NACKs, RTCP Reports and other features. If you use `webrtc.NewPeerConnection`
    // this is enabled by default. If you are manually managing You MUST create a InterceptorRegistry
    // for each PeerConnection.
    let mut registry = Registry::new();

    // Use the default set of Interceptors
    registry = register_default_interceptors(registry, &mut m)?;

    // Create the API object with the MediaEngine
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();

    // Prepare the configuration
    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Create a new RTCPeerConnection
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    // Allow us to receive 1 video track
    peer_connection
        .add_transceiver_from_kind(RTPCodecType::Video, &[])
        .await?;

    peer_connection
        .add_transceiver_from_kind(RTPCodecType::Audio, &[])
        .await?;

    let (on_negotiation_needed_sender, on_negotiation_needed_receiver) = mpsc::unbounded_channel();
    peer_connection
        .on_negotiation_needed(Box::new(move || {
            log::debug!("Negotiation needed");
            if on_negotiation_needed_sender.send(()).is_err() {
                log::warn!("Receiver closed while sending on_negotiation_needed event");
            }
            Box::pin(async {})
        }))
        .await;

    let (track_sender, track_receiver) = mpsc::unbounded_channel();
    let pc = Arc::downgrade(&peer_connection);
    peer_connection
        .on_track(Box::new(
            move |track: Option<Arc<TrackRemote>>, _receiver: Option<Arc<RTCRtpReceiver>>| {
                if let Some(track) = track {
                    // Send a PLI on an interval so that the publisher is pushing a keyframe every rtcpPLIInterval
                    // This is a temporary fix until we implement incoming RTCP events, then we would push a PLI only when a viewer requests it
                    let media_ssrc = track.ssrc();
                    let pc2 = pc.clone();
                    tokio::spawn(async move {
                        let mut result = Result::<usize>::Ok(0);
                        while result.is_ok() {
                            let timeout = tokio::time::sleep(Duration::from_secs(3));
                            tokio::pin!(timeout);
                            tokio::select! {
                                _ = timeout.as_mut() => {
                                    if let Some(pc) = pc2.upgrade() {
                                        result = pc.write_rtcp(&[Box::new(PictureLossIndication {
                                            sender_ssrc: 0,
                                            media_ssrc,
                                        })]).await.map_err(Into::into);
                                    } else {
                                        break;
                                    }
                                }
                            };
                        }
                    });
                    if track_sender.send(track).is_err() {
                        log::warn!("Dropped receiver while sending track")
                    }
                }

                Box::pin(async {})
            },
        ))
        .await;

    // Set the handler for Peer connection state
    // This will notify you when the peer has connected/disconnected
    peer_connection
        .on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            log::info!("Peer Connection State has changed: {}", s);
            Box::pin(async {})
        }))
        .await;

    peer_connection
        .on_signaling_state_change(Box::new(|a| {
            log::info!("on_signaling_state_change {a:?}");
            Box::pin(async {})
        }))
        .await;

    peer_connection
        .on_ice_connection_state_change(Box::new(|a| {
            log::info!("on_ice_connection_state_change {a:?}");
            Box::pin(async {})
        }))
        .await;

    peer_connection
        .on_ice_gathering_state_change(Box::new(|a| {
            log::info!("on_ice_gathering_state_change {a:?}");
            Box::pin(async {})
        }))
        .await;

    let (ice_candidate_sender, ice_candidate_receiver) = mpsc::unbounded_channel();
    peer_connection
        .on_ice_candidate(Box::new(move |i: Option<RTCIceCandidate>| {
            match &i {
                Some(candidate) => {
                    log::info!("On ICE Candidate: {candidate}");
                }
                None => {
                    log::info!("On ICE Candidate: None");
                }
            }
            if ice_candidate_sender.send(i).is_err() {
                log::warn!("Receiver dropped while sending ice candidate");
            }
            Box::pin(async {})
        }))
        .await;


    Ok((
        peer_connection,
        track_receiver,
        ice_candidate_receiver,
        on_negotiation_needed_receiver,
    ))
}

pub(crate) async fn create_offer(connection: &Arc<RTCPeerConnection>) -> anyhow::Result<Vec<u8>> {
    let options = RTCOfferOptions {
        ice_restart: true,
        voice_activity_detection: false,
    };
    let offer = connection.create_offer(Some(options)).await?;
    connection.set_local_description(offer).await?;
    let local_description = connection
        .local_description()
        .await
        .ok_or_else(|| anyhow::anyhow!("Empty local description"))?;
    let local_description =
        rmp_serde::to_vec(&local_description).context("serializing session description")?;
    Ok(local_description)
}

pub(crate) async fn set_answer(connection: &RTCPeerConnection, d: &[u8]) -> anyhow::Result<()> {
    let remote_description: RTCSessionDescription = rmp_serde::from_slice(d)?;
    connection
        .set_remote_description(remote_description)
        .await?;
    Ok(())
}
