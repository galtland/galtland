use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::webrtc_signaling::{self, SignalingRequest, SignalingResponse};
use galtcore::utils::ArcMutex;
use galtcore::{rmp_serde, utils};
use libp2p::PeerId;
use tokio::sync::mpsc;
use tokio::time::Duration;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
use webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver;
use webrtc::track::track_remote::TrackRemote;


#[derive(Clone)]
struct PeerState {
    connection: Arc<RTCPeerConnection>,
}

#[derive(Default, Clone)]
pub struct WebRtc {
    state: ArcMutex<HashMap<PeerId, PeerState>>,
}

impl WebRtc {
    async fn prepare_connection() -> Result<(
        Arc<RTCPeerConnection>,
        mpsc::UnboundedReceiver<Arc<TrackRemote>>,
        mpsc::UnboundedReceiver<Option<RTCIceCandidate>>,
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

        // Set a handler for when a new remote track starts, this handler copies inbound RTP packets,
        // replaces the SSRC and sends them back
        let (track_sender, track_receiver) = mpsc::unbounded_channel();
        let pc = Arc::downgrade(&peer_connection);
        peer_connection
            .on_negotiation_needed(Box::new(|| {
                log::info!("negotiation_needed");
                Box::pin(async {})
            }))
            .await;
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

        Ok((peer_connection, track_receiver, ice_candidate_receiver))
    }

    async fn initialize_or_get_state(
        self,
        peer: PeerId,
        network: &NetworkBackendClient,
    ) -> anyhow::Result<PeerState> {
        if let Some(peer_state) = self.state.lock().await.get_mut(&peer) {
            return Ok(peer_state.clone());
        }
        let (connection, mut track_receiver, mut ice_candidate_receiver) =
            Self::prepare_connection()
                .await
                .context("preparing connection")?;
        let peer_state = PeerState { connection };
        match self.state.lock().await.entry(peer) {
            hash_map::Entry::Occupied(mut o) => {
                return Ok(o.get_mut().clone());
            }
            hash_map::Entry::Vacant(e) => {
                e.insert(peer_state.clone());
            }
        };
        utils::spawn_and_log_error(async move {
            while let Some(r) = track_receiver.recv().await {
                log::info!("Received track: {r:?}");
            }
            log::info!("Track receiver for {peer} exiting...");
            Ok(())
        });
        utils::spawn_and_log_error({
            let mut network = network.clone();
            async move {
                while let Some(i) = ice_candidate_receiver.recv().await {
                    let mut ice_candidates = vec![serialize_ice_candidate(i).await?];
                    tokio::task::yield_now().await;
                    while let Ok(i) = ice_candidate_receiver.try_recv() {
                        ice_candidates.push(serialize_ice_candidate(i).await?);
                        tokio::task::yield_now().await;
                    }
                    if let Err(e) = network
                        .request_webrtc_signaling(
                            SignalingRequest {
                                offer: None,
                                ice_candidates,
                            },
                            peer,
                        )
                        .await
                        .context("request_webrtc_signaling sending ice candidates first error")? // FIXME: really fail?
                        .context("request_webrtc_signaling sending ice candidates second error")?
                    {
                        log::warn!(
                            "request_webrtc_signaling sending ice candidates last error: {e}"
                        )
                    }
                }
                log::info!("Exiting from ice candidate receiver for {peer}...");
                Ok(())
            }
        });

        Ok(peer_state)
    }

    async fn get_answer(peer_state: PeerState, offer: &[u8]) -> anyhow::Result<SignalingResponse> {
        let offer = rmp_serde::from_slice(offer)?;
        // Set the remote SessionDescription
        peer_state.connection.set_remote_description(offer).await?;

        // Create an answer
        let answer = peer_state.connection.create_answer(None).await?;

        // Sets the LocalDescription, and starts our UDP listeners
        peer_state.connection.set_local_description(answer).await?;

        let local_description = peer_state
            .connection
            .local_description()
            .await
            .ok_or_else(|| anyhow::anyhow!("Empty local description"))?;
        let answer =
            rmp_serde::to_vec(&local_description).context("serializing session description")?;
        let response = SignalingResponse {
            answer: Some(answer),
            ice_candidates: [].into(),
        };

        Ok(response)
    }

    async fn handle(
        self,
        event: webrtc_signaling::RequestEvent,
        mut network: NetworkBackendClient,
    ) -> anyhow::Result<()> {
        if event.request.offer.is_some() && !event.request.ice_candidates.is_empty() {
            anyhow::bail!(
                "Receiving both an offer and ice candidates on some request isn't currently supported (peer: {})",
                event.peer
            );
        }
        match &event.request.offer {
            Some(offer) => {
                log::info!("Handling webrtc offer for peer {}", event.peer);
                match self.initialize_or_get_state(event.peer, &network).await {
                    Ok(peer_state) => match Self::get_answer(peer_state, offer).await {
                        Ok(response) => {
                            network
                                .respond_webrtc_signaling(Ok(response), event.channel)
                                .await?;
                        }
                        Err(e) => {
                            network
                                .respond_webrtc_signaling(
                                    Err(format!("error getting answer: {e}")),
                                    event.channel,
                                )
                                .await?;
                        }
                    },
                    Err(e) => {
                        network
                            .respond_webrtc_signaling(
                                Err(format!("error initializing connection: {e}")),
                                event.channel,
                            )
                            .await?;
                    }
                }
            }
            None if !event.request.ice_candidates.is_empty() => {
                log::info!(
                    "Handling webrtc {} ice candidates for peer {}",
                    event.request.ice_candidates.len(),
                    event.peer
                );
                let success = match self.state.lock().await.get_mut(&event.peer) {
                    Some(peer_state) => {
                        for candidate in &event.request.ice_candidates {
                            let candidate = match candidate {
                                Some(candidate) => {
                                    let candidate: RTCIceCandidateInit =
                                        rmp_serde::from_slice(candidate)
                                            .context("deserializing rtc ice candidate init")?;
                                    candidate
                                }
                                None => Default::default(),
                            };
                            peer_state.connection.add_ice_candidate(candidate).await?;
                        }
                        true
                    }
                    None => false,
                };
                if success {
                    network
                        .respond_webrtc_signaling(Ok(Default::default()), event.channel)
                        .await?;
                } else {
                    let message = format!(
                        "Received ice candidates from {} but no connection open!",
                        event.peer
                    );
                    network
                        .respond_webrtc_signaling(Err(message.clone()), event.channel)
                        .await?;
                    anyhow::bail!("{}", message);
                }
            }
            None => {
                let message = format!(
                    "Received an empty request: {:?} from {}",
                    event.request, event.peer
                );
                network
                    .respond_webrtc_signaling(Err(message.clone()), event.channel)
                    .await?;
                anyhow::bail!("{}", message);
            }
        }
        Ok(())
    }

    pub async fn webrtc_signaling_loop(
        mut receiver: mpsc::UnboundedReceiver<webrtc_signaling::RequestEvent>,
        network: NetworkBackendClient,
    ) -> anyhow::Result<()> {
        let this: WebRtc = Default::default();
        while let Some(event) = receiver.recv().await {
            utils::spawn_and_log_error(this.clone().handle(event, network.clone()));
            tokio::task::yield_now().await;
        }
        log::info!("Exiting from webrtc signaling loop...");
        Ok(())
    }
}


async fn serialize_ice_candidate(i: Option<RTCIceCandidate>) -> anyhow::Result<Option<Vec<u8>>> {
    match i {
        Some(i) => {
            let init = i.to_json().await?;
            rmp_serde::to_vec(&init)
                .context("serializing ice candidate init")
                .map(Some)
        }
        None => Ok(None),
    }
}
