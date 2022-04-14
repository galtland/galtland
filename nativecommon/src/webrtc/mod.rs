// SPDX-License-Identifier: AGPL-3.0-only

mod play_stream;
mod publish_stream;
mod setup;

use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use anyhow::Context;
use galtcore::daemons::cm::ClientCommand;
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::delegated_streaming::{
    self, DelegatedStreamingRequest, DelegatedStreamingResponse, WebRtcStream, WebRtcTrack,
};
use galtcore::protocols::media_streaming::{MyRTCPFeedback, MyRTCRtpCodecCapability, StreamingKey};
use galtcore::protocols::webrtc_signaling::{self, SignalingRequestOrResponse};
use galtcore::protocols::NodeIdentity;
use galtcore::utils::ArcMutex;
use galtcore::{bincode, utils};
use itertools::Itertools;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_gathering_state::RTCIceGatheringState;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::track::track_remote::TrackRemote;

use self::publish_stream::DelegatedStreamingPublishingState;

#[derive(Clone)]
struct PeerState {
    connection: Arc<RTCPeerConnection>,
    publishing_state: ArcMutex<publish_stream::DelegatedStreamingPublishingState>,
    playing_state: ArcMutex<HashMap<StreamingKey, play_stream::DelegatedStreamingPlayingState>>,
}

#[derive(Default, Clone)]
pub struct WebRtc {
    state: ArcMutex<HashMap<PeerId, PeerState>>,
}

impl WebRtc {
    async fn initialize_or_get_state(
        self,
        peer: PeerId,
        network: &NetworkBackendClient,
        commands: mpsc::Sender<ClientCommand>,
        identity: NodeIdentity,
    ) -> anyhow::Result<PeerState> {
        {
            let mut state = self.state.lock().await;
            if let Some(peer_state) = state.get_mut(&peer) {
                match peer_state.connection.connection_state() {
                    RTCPeerConnectionState::Unspecified
                    | RTCPeerConnectionState::New
                    | RTCPeerConnectionState::Connecting
                    | RTCPeerConnectionState::Connected => {
                        return Ok(peer_state.clone());
                    }
                    bad_state @ (RTCPeerConnectionState::Disconnected
                    | RTCPeerConnectionState::Failed
                    | RTCPeerConnectionState::Closed) => {
                        log::info!("WebRTC connection to {peer} is in bad state: {bad_state:?}, creating a new one...");
                        state.remove(&peer);
                    }
                }
            }
        }
        let (
            connection,
            track_receiver,
            ice_candidate_receiver,
            mut on_negotiation_needed_receiver,
        ) = setup::prepare_connection()
            .await
            .context("preparing connection")?;
        let peer_state = PeerState {
            connection: Arc::clone(&connection),
            playing_state: Arc::new(Mutex::new(HashMap::new())),
            publishing_state: Arc::new(Mutex::new(
                DelegatedStreamingPublishingState::NotPublishing,
            )),
        };
        match self.state.lock().await.entry(peer) {
            hash_map::Entry::Occupied(mut o) => {
                return Ok(o.get_mut().clone());
            }
            hash_map::Entry::Vacant(e) => {
                e.insert(peer_state.clone());
            }
        };
        utils::spawn_and_log_error(track_receiver_loop(
            peer,
            peer_state.clone(),
            track_receiver,
            commands,
            identity,
        ));
        utils::spawn_and_log_error(ice_candidate_receiver_loop(
            peer,
            network.clone(),
            ice_candidate_receiver,
        ));
        utils::spawn_and_log_error({
            let network = network.clone();
            let peer_state = peer_state.clone();
            async move {
                while on_negotiation_needed_receiver.recv().await.is_some() {
                    match connection.ice_gathering_state() {
                        RTCIceGatheringState::Unspecified
                        | RTCIceGatheringState::Gathering
                        | RTCIceGatheringState::New => {
                            log::info!(
                                "Will skip renegotiation with {peer}, connection: {:?}, signaling: {:?}, ice connection: {:?}, ice gathering: {:?}",
                                connection.connection_state(),
                                connection.signaling_state(),
                                connection.ice_connection_state(),
                                connection.ice_gathering_state()
                            );
                            continue;
                        }
                        RTCIceGatheringState::Complete => {
                            log::info!(
                                "Will try to renegotiate with {peer}, connection: {:?}, signaling: {:?}, ice connection: {:?}, ice gathering: {:?}",
                                connection.connection_state(),
                                connection.signaling_state(),
                                connection.ice_connection_state(),
                                connection.ice_gathering_state()
                            )
                        }
                    };
                    let description = match setup::create_offer(&connection).await {
                        Ok(d) => d,
                        Err(e) => {
                            close_connection(&peer_state.connection, peer).await;
                            anyhow::bail!("Error creating offer: {e:?}");
                        }
                    };
                    match network
                        .request_webrtc_signaling(
                            SignalingRequestOrResponse {
                                description: Some(description),
                                ice_candidates: Vec::new(),
                            },
                            peer,
                        )
                        .await
                    {
                        Ok(Ok(Ok(r))) => {
                            if let Some(d) = r.description {
                                if let Err(e) = setup::set_answer(&connection, &d).await {
                                    close_connection(&peer_state.connection, peer).await;
                                    anyhow::bail!("Error processing answer: {e:?}");
                                }
                                if let Err(e) =
                                    process_ice_candidates(&peer_state, &r.ice_candidates).await
                                {
                                    close_connection(&peer_state.connection, peer).await;
                                    anyhow::bail!(
                                        "Error processing ice candidates after receiving answer: {e:?}"
                                    );
                                }
                            } else {
                                close_connection(&peer_state.connection, peer).await;
                                anyhow::bail!("Expected an answer but got None");
                            }
                        }
                        Ok(Ok(Err(e))) => {
                            close_connection(&peer_state.connection, peer).await;
                            anyhow::bail!("Error receiving offer answer: {e:?}");
                        }
                        Ok(Err(e)) => {
                            close_connection(&peer_state.connection, peer).await;
                            anyhow::bail!("Error receiving offer answer, outbound failure: {e:?}");
                        }
                        Err(e) => {
                            close_connection(&peer_state.connection, peer).await;
                            anyhow::bail!("Error receiving offer answer: {e:?}");
                        }
                    };
                    log::debug!("Successfully sent offer and received WebRTC answer from {peer}");
                    tokio::task::yield_now().await;
                }
                log::info!("Exiting from on negotiation needed loop...");
                Ok(())
            }
        });
        Ok(peer_state)
    }

    async fn get_answer(
        peer_state: PeerState,
        offer: &[u8],
    ) -> anyhow::Result<SignalingRequestOrResponse> {
        let offer = bincode::deserialize(offer)?;
        peer_state.connection.set_remote_description(offer).await?;
        let answer = peer_state.connection.create_answer(None).await?;
        // Sets the LocalDescription, and starts our UDP listeners
        peer_state.connection.set_local_description(answer).await?;
        let local_description = peer_state
            .connection
            .local_description()
            .await
            .ok_or_else(|| anyhow::anyhow!("Empty local description"))?;
        let answer =
            bincode::serialize(&local_description).context("serializing session description")?;
        let response = SignalingRequestOrResponse {
            description: Some(answer),
            ice_candidates: Vec::new(),
        };
        Ok(response)
    }

    async fn handle_webrtc_signaling(
        self,
        event: webrtc_signaling::RequestEvent,
        network: NetworkBackendClient,
        commands: mpsc::Sender<ClientCommand>,
        identity: NodeIdentity,
    ) -> anyhow::Result<()> {
        let peer = event.peer;
        if event.request.description.is_some() && !event.request.ice_candidates.is_empty() {
            anyhow::bail!(
                "Receiving both an offer and ice candidates on some request isn't currently supported (peer: {peer})"
            );
        }
        match &event.request.description {
            Some(offer) => {
                log::info!("Handling webrtc offer for peer {peer}");
                match self
                    .initialize_or_get_state(peer, &network, commands, identity)
                    .await
                {
                    Ok(peer_state) => match Self::get_answer(peer_state.clone(), offer).await {
                        Ok(response) => {
                            network
                                .respond_webrtc_signaling(Ok(response), event.channel)
                                .await?;
                        }
                        Err(e) => {
                            close_connection(&peer_state.connection, peer).await;
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
                    "Handling webrtc {} ice candidates for peer {peer}",
                    event.request.ice_candidates.len(),
                );
                let success = match self.state.lock().await.get_mut(&peer) {
                    Some(peer_state) => {
                        process_ice_candidates(peer_state, &event.request.ice_candidates).await?;
                        true
                    }
                    None => false,
                };
                if success {
                    network
                        .respond_webrtc_signaling(Ok(Default::default()), event.channel)
                        .await?;
                } else {
                    let message =
                        format!("Received ice candidates from {peer} but no connection open!");
                    network
                        .respond_webrtc_signaling(Err(message.clone()), event.channel)
                        .await?;
                    anyhow::bail!("{}", message);
                }
            }
            None => {
                let message = format!("Received an empty request: {:?} from {peer}", event.request);
                network
                    .respond_webrtc_signaling(Err(message.clone()), event.channel)
                    .await?;
                anyhow::bail!("{}", message);
            }
        }
        Ok(())
    }

    async fn handle_delegated_streaming_event(
        self,
        event: delegated_streaming::RequestEvent,
        network: NetworkBackendClient,
        commands: mpsc::Sender<ClientCommand>,
        identity: NodeIdentity,
    ) -> anyhow::Result<()> {
        let peer = event.peer;
        let peer_state = self.state.lock().await.get_mut(&peer).cloned();
        if let Some(peer_state) = peer_state {
            match event.request {
                DelegatedStreamingRequest::PublishStream(info) => {
                    log::info!("Received publish stream info: {info:?}");
                    let mut publishing_state = peer_state.publishing_state.lock().await;
                    *publishing_state = match &*publishing_state {
                        DelegatedStreamingPublishingState::NotPublishing => {
                            DelegatedStreamingPublishingState::GatheringInfo {
                                pending_remote_tracks: HashMap::new(),
                                pending_publishing_info: Some(info),
                            }
                        }
                        DelegatedStreamingPublishingState::GatheringInfo {
                            pending_remote_tracks,
                            pending_publishing_info: _,
                        } => {
                            if info.tracks.iter().counts_by(|w| w.stream_id.clone())
                                == pending_remote_tracks
                                    .iter()
                                    .counts_by(|(w, _)| w.stream_id.clone())
                            {
                                match publish_stream::publish(
                                    info,
                                    pending_remote_tracks,
                                    &commands,
                                    identity,
                                )
                                .await
                                {
                                    Ok(s) => s,
                                    Err(e) => {
                                        let message = format!("Error publishing stream: {e:?}");
                                        if let Err(e) = network
                                            .respond_delegated_streaming(
                                                Err(message.clone()),
                                                event.channel,
                                            )
                                            .await
                                        {
                                            log::error!("Error responding delegated streaming '{message}': {e:?}")
                                        };
                                        anyhow::bail!("{}", message)
                                    }
                                }
                            } else {
                                DelegatedStreamingPublishingState::GatheringInfo {
                                    pending_remote_tracks: pending_remote_tracks.clone(),
                                    pending_publishing_info: Some(info),
                                }
                            }
                        }
                        DelegatedStreamingPublishingState::Publishing { info, .. } => {
                            let message = format!("Already publishing {:?}", info.streaming_key);
                            if let Err(e) = network
                                .respond_delegated_streaming(Err(message.clone()), event.channel)
                                .await
                            {
                                log::error!(
                                    "Error responding delegated streaming '{message}': {e:?}"
                                )
                            };
                            anyhow::bail!("{}", message)
                        }
                    };
                }
                DelegatedStreamingRequest::PlayStream { streaming_key } => {
                    match peer_state
                        .playing_state
                        .lock()
                        .await
                        .entry(streaming_key.clone())
                    {
                        hash_map::Entry::Occupied(e) => {
                            log::warn!(
                                    "Peer {:?} is trying to play streaming {:?} but already playing: {:?}",
                                    peer,
                                    streaming_key,
                                    e.get().tracks.lock().await.keys()
                                )
                        }
                        hash_map::Entry::Vacant(e) => {
                            let new_state = play_stream::play(
                                peer,
                                streaming_key,
                                peer_state.clone(),
                                commands,
                                network.clone(),
                            )
                            .await?;
                            e.insert(new_state);
                        }
                    };
                }
                DelegatedStreamingRequest::PlayStreamNewTrack(i) => {
                    log::error!("We received PlayStreamNewTrack but we are supposed to only send it! {i:?} {peer}");
                }
            };
        } else {
            let message = "No webrtc connection established, will not handle request".to_string();
            if let Err(e) = network
                .respond_delegated_streaming(Err(message.clone()), event.channel)
                .await
            {
                log::error!("Error responding delegated streaming '{message}': {e:?}")
            };
            anyhow::bail!("{}", message)
        }
        network
            .respond_delegated_streaming(Ok(DelegatedStreamingResponse {}), event.channel)
            .await?;
        Ok(())
    }

    pub async fn webrtc_main_loop(
        mut webrtc_signaling_receiver: mpsc::UnboundedReceiver<webrtc_signaling::RequestEvent>,
        mut delegated_streaming_receiver: mpsc::UnboundedReceiver<
            delegated_streaming::RequestEvent,
        >,
        network: NetworkBackendClient,
        commands: mpsc::Sender<ClientCommand>,
        identity: NodeIdentity,
    ) -> anyhow::Result<()> {
        let this: WebRtc = Default::default();
        loop {
            tokio::task::yield_now().await;
            tokio::select! {
                event = webrtc_signaling_receiver.recv() => {
                    if let Some(event) = event {
                        utils::spawn_and_log_error(this.clone().handle_webrtc_signaling(event, network.clone(), commands.clone(), identity.clone()));
                    } else {
                        log::warn!("None received from webrtc_signaling_receiver")
                    }
                },
                event = delegated_streaming_receiver.recv() => {
                    if let Some(event) = event {
                        utils::spawn_and_log_error(this.clone().handle_delegated_streaming_event(event, network.clone(), commands.clone(), identity.clone()));
                    } else {
                        log::warn!("None received from delegated_streaming_receiver")
                    }
                }
            }
        }
        // log::info!("Exiting from webrtc main loop...");
        // Ok(())
    }
}

async fn close_connection(connection: &Arc<RTCPeerConnection>, peer: PeerId) {
    if let Err(e) = connection.close().await {
        log::warn!("Error closing connection to {peer}: {e:?}");
    }
}

async fn process_ice_candidates(
    peer_state: &PeerState,
    ice_candidates: &Vec<Option<Vec<u8>>>,
) -> anyhow::Result<()> {
    for candidate in ice_candidates {
        let candidate = match candidate {
            Some(candidate) => {
                let candidate: RTCIceCandidateInit = bincode::deserialize(candidate)
                    .context("deserializing rtc ice candidate init")?;
                candidate
            }
            None => Default::default(),
        };
        peer_state.connection.add_ice_candidate(candidate).await?;
    }
    Ok(())
}

fn from_codec_capability(
    c: webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
) -> MyRTCRtpCodecCapability {
    MyRTCRtpCodecCapability {
        mime_type: c.mime_type,
        clock_rate: c.clock_rate,
        channels: c.channels,
        sdp_fmtp_line: c.sdp_fmtp_line,
        rtcp_feedback: c
            .rtcp_feedback
            .iter()
            .map(|r| MyRTCPFeedback {
                parameter: r.parameter.to_owned(),
                typ: r.typ.to_owned(),
            })
            .collect(),
    }
}


fn to_codec_capability(
    c: MyRTCRtpCodecCapability,
) -> webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability {
    webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability {
        mime_type: c.mime_type,
        clock_rate: c.clock_rate,
        channels: c.channels,
        sdp_fmtp_line: c.sdp_fmtp_line,
        rtcp_feedback: c
            .rtcp_feedback
            .iter()
            .map(|r| webrtc::rtp_transceiver::RTCPFeedback {
                parameter: r.parameter.to_owned(),
                typ: r.typ.to_owned(),
            })
            .collect(),
    }
}


async fn track_receiver_loop(
    peer: PeerId,
    peer_state: PeerState,
    mut track_receiver: mpsc::UnboundedReceiver<Arc<TrackRemote>>,
    commands: mpsc::Sender<ClientCommand>,
    identity: NodeIdentity,
) -> anyhow::Result<()> {
    while let Some(t) = track_receiver.recv().await {
        let k = WebRtcTrack {
            stream_id: WebRtcStream(t.stream_id().await),
            track_id: t.id().await,
        };
        log::info!("Received track: {t:?} indexing by {k:?}");
        let mut publishing_state = peer_state.publishing_state.lock().await;
        *publishing_state = match &mut *publishing_state {
            DelegatedStreamingPublishingState::NotPublishing => {
                let mut pending_remote_tracks = HashMap::new();
                pending_remote_tracks.insert(k, t);
                log::debug!(
                    "Putting first track in pending list because not publishing info received yet"
                );
                DelegatedStreamingPublishingState::GatheringInfo {
                    pending_remote_tracks,
                    pending_publishing_info: None,
                }
            }
            DelegatedStreamingPublishingState::GatheringInfo {
                pending_remote_tracks,
                pending_publishing_info,
            } => {
                pending_remote_tracks.insert(k, t);
                match pending_publishing_info {
                    Some(info) => {
                        if info.tracks.iter().counts_by(|w| w.stream_id.clone())
                            == pending_remote_tracks
                                .iter()
                                .counts_by(|(w, _)| w.stream_id.clone())
                        {
                            log::debug!(
                                "Last track received, beginning publishing for {}",
                                info.streaming_key
                            );
                            publish_stream::publish(
                                info.clone(),
                                pending_remote_tracks,
                                &commands,
                                identity.clone(),
                            )
                            .await?
                        } else {
                            log::debug!("Track received but still some pending, known info tracks: {}, pending tracks: {}",
                                info.tracks.len(), pending_remote_tracks.len());
                            DelegatedStreamingPublishingState::GatheringInfo {
                                pending_remote_tracks: pending_remote_tracks.clone(),
                                pending_publishing_info: pending_publishing_info.clone(),
                            }
                        }
                    }
                    None => {
                        log::debug!(
                            "Track received but publishing info, pending tracks: {}",
                            pending_remote_tracks.len()
                        );
                        DelegatedStreamingPublishingState::GatheringInfo {
                            pending_remote_tracks: pending_remote_tracks.clone(),
                            pending_publishing_info: pending_publishing_info.clone(),
                        }
                    }
                }
            }
            DelegatedStreamingPublishingState::Publishing { info, tracks } => {
                log::warn!(
                    "Already publishing {:?} but received new track, ignoring...",
                    info.streaming_key
                );
                DelegatedStreamingPublishingState::Publishing {
                    info: info.to_owned(),
                    tracks: tracks.to_owned(),
                }
            }
        };
        tokio::task::yield_now().await;
    }
    log::info!("Track receiver for {peer} exiting...");
    Ok(())
}


async fn ice_candidate_receiver_loop(
    peer: PeerId,
    network: NetworkBackendClient,
    mut ice_candidate_receiver: mpsc::UnboundedReceiver<Option<RTCIceCandidate>>,
) -> anyhow::Result<()> {
    while let Some(i) = ice_candidate_receiver.recv().await {
        let mut ice_candidates = vec![serialize_ice_candidate(i).await?];
        tokio::task::yield_now().await;
        while let Ok(i) = ice_candidate_receiver.try_recv() {
            ice_candidates.push(serialize_ice_candidate(i).await?);
            tokio::task::yield_now().await;
        }
        if let Err(e) = network
            .request_webrtc_signaling(
                SignalingRequestOrResponse {
                    description: None,
                    ice_candidates,
                },
                peer,
            )
            .await
            .context("request_webrtc_signaling sending ice candidates first error")? // FIXME: really fail?
            .context("request_webrtc_signaling sending ice candidates second error")?
        {
            log::warn!("request_webrtc_signaling sending ice candidates last error: {e}")
        }
        tokio::task::yield_now().await;
    }
    log::info!("Exiting from ice candidate receiver for {peer}...");
    Ok(())
}


async fn serialize_ice_candidate(i: Option<RTCIceCandidate>) -> anyhow::Result<Option<Vec<u8>>> {
    match i {
        Some(i) => {
            let init = i.to_json().await?;
            bincode::serialize(&init)
                .context("serializing ice candidate init")
                .map(Some)
        }
        None => Ok(None),
    }
}
