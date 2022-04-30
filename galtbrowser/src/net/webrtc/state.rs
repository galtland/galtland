// SPDX-License-Identifier: AGPL-3.0-only


use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::Context;
use galtcore::daemons::internal_network_events::BroadcastableNetworkEvent;
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::delegated_streaming::{
    self, DelegatedStreamingRequest, PlayStreamNewTrackInfo, WebRtcStream,
};
use galtcore::protocols::media_streaming::StreamingKey;
use galtcore::protocols::webrtc_signaling::{self, SignalingRequestOrResponse};
use galtcore::tokio::sync::{broadcast, mpsc, Mutex};
use galtcore::utils::ArcMutex;
use galtcore::{bincode, tokio};
use libp2p::{Multiaddr, PeerId};
use rand::Rng;
use web_sys::{MediaStream, MediaStreamTrack, RtcPeerConnection};

use super::setup::{self, MyRTCIceCandidateInit};
use crate::net::ConnectionStatusUpdate;


struct PlayStreamState {
    playing_tracks: HashMap<WebRtcStream, (MediaStream, MediaStreamTrack)>,
}

type PlayingStreamsType = ArcMutex<HashMap<StreamingKey, PlayStreamState>>;

#[derive(Clone)]
struct ConnectionState {
    delegated_streaming_peer: PeerId,
    connection: Rc<RtcPeerConnection>,
    pending_webrtc_tracks: ArcMutex<HashMap<WebRtcStream, StreamingKey>>,
    pending_media_stream_tracks: ArcMutex<HashMap<WebRtcStream, (MediaStream, MediaStreamTrack)>>,
    playing_streams: PlayingStreamsType,
}

type ConnectionStateType = ArcMutex<Option<ConnectionState>>;

#[derive(Clone)]
pub(crate) struct WebRtcState {
    connection_state: ConnectionStateType,
    network: NetworkBackendClient,
}

impl WebRtcState {
    pub(crate) fn new(network: NetworkBackendClient) -> Self {
        Self {
            connection_state: Arc::new(Mutex::new(None)),
            network,
        }
    }

    pub(crate) async fn run_receive_loop(
        self,
        mut network_event_receiver: broadcast::Receiver<BroadcastableNetworkEvent>,
        mut webrtc_request_event_receiver: mpsc::UnboundedReceiver<webrtc_signaling::RequestEvent>,
        mut delegated_streaming_event_receiver: mpsc::UnboundedReceiver<
            delegated_streaming::RequestEvent,
        >,
        connection_status_update_sender: mpsc::UnboundedSender<ConnectionStatusUpdate>,
        delegated_streaming_endpoint: Multiaddr, // mut delegated_streaming_event_receiver: mpsc::UnboundedReceiver<
                                                 //     delegated_streaming::RequestEvent,
                                                 // >
    ) -> anyhow::Result<()> {
        loop {
            let network = self.network.clone();
            let connection_status_update_sender = connection_status_update_sender.clone();
            log::debug!("Looping receive loop");
            tokio::select! {
                event = network_event_receiver.recv() => {
                    match event.context("receiving broadcast")? {
                        BroadcastableNetworkEvent::ReceivedGossip { .. } => {}
                        BroadcastableNetworkEvent::PutRecord { .. } => {}
                        BroadcastableNetworkEvent::SentStreamingResponseStats { .. } => {}
                        BroadcastableNetworkEvent::PingInfo { .. } => {}
                        BroadcastableNetworkEvent::IdentifyInfo { .. } => {}
                        BroadcastableNetworkEvent::ConnectionEstablished { peer, endpoint } if endpoint == delegated_streaming_endpoint => {
                            if self.connection_state.lock().await.is_some() {
                                log::info!("Reconnecting to {peer} but webrtc connection has been already established");
                            } else  {
                                log::info!("Handling connection established for {peer}");
                                let connection_state = self.connection_state.clone();
                                galtcore::utils::wspawn_and_log_error(async move {
                                    Self::handle_connection_established(connection_state, peer, network, connection_status_update_sender).await?;
                                    Ok(())
                                });
                            }
                        },
                        BroadcastableNetworkEvent::ConnectionEstablished { .. }  => {}
                    };
                },
                event = webrtc_request_event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            match self.connection_state.lock().await.as_ref() {
                                Some(connection_state) => {
                                    log::info!(
                                        "Handling event request from {} with {} ice candidates and offer? {}",
                                        event.peer,
                                        event.request.ice_candidates.len(),
                                        event.request.description.is_some()
                                    );
                                    let connection = connection_state.connection.clone();
                                    let network = network.clone();
                                    galtcore::utils::wspawn_and_log_error(async move {
                                        Self::handle_request_event(event, connection, network).await?;
                                        Ok(())
                                    });
                                },
                                None => {
                                    log::warn!("Received request event but no connection yet established: {:?} for {}", event.request, event.peer);
                                },
                            };
                        },
                        None => {
                            log::warn!("Got None while receiving event");
                        }
                    };
                },
                event = delegated_streaming_event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            let peer = event.peer;
                            match event.request {
                                DelegatedStreamingRequest::PlayStreamNewTrack(info) => {
                                    match self.connection_state.lock().await.as_ref() {
                                        Some(connection_state) => {
                                            let streaming_key = info.streaming_key.clone();
                                            log::info!(
                                                "Handling PlayStreamNewTrack request from {peer} for {streaming_key} with {} tracks",
                                                info.tracks.len(),
                                            );
                                            let connection_state = connection_state.clone();
                                            galtcore::utils::wspawn_and_log_error(async move {
                                                match handle_delegated_streaming_event(info, connection_state).await {
                                                    Ok(_) => {
                                                        log::info!("Successfully processed PlayStreamNewTrack from {peer} for {streaming_key}");
                                                        if let Err(e) = network.respond_delegated_streaming(Ok(Default::default()), event.channel).await {
                                                            anyhow::bail!("Error responding success delegated streaming to {peer}: {e}")
                                                        }
                                                    },
                                                    Err(e) => {
                                                        let message = format!("Error handling delegated streaming event from {peer}: {e:?}");
                                                        log::warn!("{}", message);
                                                        if let Err(e) = network
                                                            .respond_delegated_streaming(
                                                                Err(message.clone()),
                                                                event.channel,
                                                            )
                                                            .await
                                                        {
                                                            log::error!("Error responding delegated streaming '{message}': {e:?}")
                                                        };
                                                    }
                                                }
                                                Ok(())
                                            });
                                        },
                                        None => {
                                            log::warn!("Received PlayStreamNewTrack request but no connection yet established: {info:?} for {peer}");
                                        },
                                    };
                                }
                                other @ (
                                    DelegatedStreamingRequest::PlayStream {..}
                                    | DelegatedStreamingRequest::PublishStream(_)
                                ) => log::error!("Received unknown event from {peer}: {other:?}")
                            }
                        },
                        None => {
                            log::warn!("Got None while receiving event");
                        }
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    }

    async fn handle_connection_established(
        connection_state: ConnectionStateType,
        delegated_streaming_peer: PeerId,
        network: NetworkBackendClient,
        connection_status_update_sender: mpsc::UnboundedSender<ConnectionStatusUpdate>,
    ) -> anyhow::Result<()> {
        let (
            connection,
            mut ice_candidates_receiver,
            mut on_negotiation_needed_receiver,
            mut on_track_receiver,
        ) = setup::webrtc_initialize(connection_status_update_sender);

        let connection_state = {
            let mut connection_state_option = connection_state.lock().await;
            if connection_state_option.is_some() {
                anyhow::bail!("Some concurrency issue while initializing webrtc for {delegated_streaming_peer}")
            } else {
                let connection_state = ConnectionState {
                    delegated_streaming_peer,
                    connection: connection.clone(),
                    pending_webrtc_tracks: Arc::new(Mutex::new(HashMap::new())),
                    pending_media_stream_tracks: Arc::new(Mutex::new(HashMap::new())),
                    playing_streams: Arc::new(Mutex::new(HashMap::new())),
                };
                connection_state_option.replace(connection_state.clone());
                connection_state
            }
        };

        galtcore::utils::wspawn_and_log_error({
            async move {
                while let Some(track_event) = on_track_receiver.recv().await {
                    let media_stream_track = track_event.track();
                    let media_stream: MediaStream = {
                        let streams = track_event.streams();
                        if streams.length() != 1 {
                            anyhow::bail!("Received track that is included in {} streams, which is unsupported", streams.length());
                        }
                        streams.get(0).into()
                    };

                    let webrtc_stream = WebRtcStream(media_stream.id());
                    match connection_state
                        .pending_webrtc_tracks
                        .lock()
                        .await
                        .get(&webrtc_stream)
                    {
                        Some(streaming_key) => {
                            log::info!("Found {streaming_key}, will start playing with received media stream");
                            really_start_playing(
                                webrtc_stream,
                                media_stream,
                                media_stream_track,
                                streaming_key.clone(),
                                Arc::clone(&connection_state.playing_streams),
                            )
                            .await;
                        }
                        None => {
                            log::info!("Saving media stream {webrtc_stream:?} because no streaming key found");
                            connection_state
                                .pending_media_stream_tracks
                                .lock()
                                .await
                                .insert(webrtc_stream, (media_stream, media_stream_track));
                        }
                    };
                    tokio::task::yield_now().await;
                }
                log::info!("Exiting from on_track_receiver loop...");
                Ok(())
            }
        });

        galtcore::utils::wspawn_and_log_error({
            let network = network.clone();
            async move {
                Self::create_offer(delegated_streaming_peer, &connection, &network).await?;
                while on_negotiation_needed_receiver.recv().await.is_some() {
                    Self::create_offer(delegated_streaming_peer, &connection, &network).await?;
                    tokio::task::yield_now().await;
                }
                log::info!(
                    "Exiting from on negotiation needed receiver for {delegated_streaming_peer}..."
                );
                Ok(())
            }
        });
        galtcore::utils::wspawn_and_log_error({
            let network = network.clone();
            async move {
                while let Some(i) = ice_candidates_receiver.recv().await {
                    let mut ice_candidates = Vec::new();
                    match &i {
                        Some(i) => ice_candidates.push(Some(Self::serialize_ice_candidate(i)?)),
                        None => ice_candidates.push(None),
                    };
                    tokio::task::yield_now().await;
                    while let Ok(i) = ice_candidates_receiver.try_recv() {
                        match &i {
                            Some(i) => ice_candidates.push(Some(Self::serialize_ice_candidate(i)?)),
                            None => ice_candidates.push(None),
                        };
                        tokio::task::yield_now().await;
                    }
                    log::info!(
                        "Sending {} ice candidates to {delegated_streaming_peer}",
                        ice_candidates.len()
                    );
                    if let Err(e) = network
                        .request_webrtc_signaling(
                            SignalingRequestOrResponse {
                                description: None,
                                ice_candidates,
                            },
                            delegated_streaming_peer,
                        )
                        .await
                        .context("request_webrtc_signaling sending ice candidates first error")? // FIXME: really fail?
                        .context("request_webrtc_signaling sending ice candidates second error")?
                    {
                        log::warn!(
                            "request_webrtc_signaling sending ice candidates last error: {e}"
                        )
                    };
                    tokio::task::yield_now().await;
                }
                log::info!(
                    "Exiting from ice candidate receiver from {delegated_streaming_peer}..."
                );
                Ok(())
            }
        });
        Ok(())
    }

    async fn handle_request_event(
        event: webrtc_signaling::RequestEvent,
        connection: Rc<RtcPeerConnection>,
        network: NetworkBackendClient,
    ) -> anyhow::Result<()> {
        let peer = event.peer;
        if event.request.ice_candidates.is_empty() && event.request.description.is_none() {
            let message = format!("Pointless empty request from {peer}");
            network
                .respond_webrtc_signaling(Err(message.clone()), event.channel)
                .await?;
            anyhow::bail!("{}", message);
        }
        let description = match event.request.description {
            Some(remote_description) => {
                match setup::get_answer(&connection, &remote_description).await {
                    Ok(answer) => {
                        log::info!("Setting remote description and returning answer");
                        Some(answer)
                    }
                    Err(e) => {
                        let message = format!("Error creating answer for offer from {peer}: {e:?}");
                        network
                            .respond_webrtc_signaling(Err(message.clone()), event.channel)
                            .await?;
                        anyhow::bail!("{}", message);
                    }
                }
            }
            None => None,
        };

        for candidate in &event.request.ice_candidates {
            let my_ice_candidate = match candidate {
                Some(candidate) => {
                    let my_ice_candidate: MyRTCIceCandidateInit =
                        bincode::deserialize(candidate)
                            .context("deserializing ice candidate init")?;
                    Some(my_ice_candidate)
                }
                None => None,
            };
            setup::add_ice_candidate(&connection, my_ice_candidate).await?;
        }
        network
            .respond_webrtc_signaling(
                Ok(SignalingRequestOrResponse {
                    description,
                    ice_candidates: Vec::new(),
                }),
                event.channel,
            )
            .await?;

        Ok(())
    }

    fn serialize_ice_candidate(c: &MyRTCIceCandidateInit) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(c).context("serializing rtc ice candidate")
    }

    async fn create_offer(
        peer: PeerId,
        connection: &Rc<RtcPeerConnection>,
        network: &NetworkBackendClient,
    ) -> anyhow::Result<()> {
        let session_description = setup::create_offer(connection).await?;
        let offer =
            bincode::serialize(&session_description).context("serializing session description")?;
        let answer = match network
            .request_webrtc_signaling(
                SignalingRequestOrResponse {
                    description: Some(offer),
                    ice_candidates: Default::default(),
                },
                peer,
            )
            .await
            .context("request_webrtc_signaling sending offer first error")?
            .context("request_webrtc_signaling sending offer second error")?
        {
            Ok(r) => r.description,
            Err(e) => {
                anyhow::bail!("request_webrtc_signaling sending offer last error: {e}")
            }
        };

        let answer: setup::MySessionDescription = bincode::deserialize(
            &answer.ok_or_else(|| anyhow::anyhow!("received none as answer"))?,
        )
        .context("deserializing session description")?;
        setup::receive_answer(connection, &answer).await?;

        Ok(())
    }

    pub(crate) async fn share_screen(&mut self) -> anyhow::Result<StreamingKey> {
        let display_media = setup::get_display_media().await?;
        if let Some(connection_state) = self.connection_state.lock().await.as_ref() {
            let tracks = setup::share_screen(&display_media, &connection_state.connection).await?;
            let channel_key = connection_state.delegated_streaming_peer;
            let video_key = rand::thread_rng().gen::<[u8; 32]>();
            let streaming_key = StreamingKey {
                video_key: video_key.into(),
                channel_key,
            };
            let _ = match self
                .network
                .request_delegated_streaming(
                    DelegatedStreamingRequest::PublishStream(
                        galtcore::protocols::delegated_streaming::PublishStreamInfo {
                            streaming_key: streaming_key.clone(),
                            tracks,
                        },
                    ),
                    connection_state.delegated_streaming_peer,
                )
                .await
                .context("request_delegated_streaming sending share screen first error")?
                .context("request_delegated_streaming sending share screen second error")?
            {
                Ok(response) => response,
                Err(e) => {
                    anyhow::bail!(
                        "request_delegated_streaming sending share screen last error: {e}"
                    )
                }
            };
            Ok(streaming_key)
        } else {
            anyhow::bail!("Trying to share screen but no active webrtc connection");
        }
    }

    pub(crate) async fn share_audio_video(&mut self) -> anyhow::Result<StreamingKey> {
        let user_media = setup::get_user_media().await?;
        if let Some(connection_state) = self.connection_state.lock().await.as_ref() {
            let tracks =
                setup::share_audio_video(&user_media, &connection_state.connection).await?;
            let channel_key = connection_state.delegated_streaming_peer;
            let video_key = rand::thread_rng().gen::<[u8; 32]>();
            let streaming_key = StreamingKey {
                video_key: video_key.into(),
                channel_key,
            };
            let _ = match self
                .network
                .request_delegated_streaming(
                    DelegatedStreamingRequest::PublishStream(
                        galtcore::protocols::delegated_streaming::PublishStreamInfo {
                            streaming_key: streaming_key.clone(),
                            tracks,
                        },
                    ),
                    connection_state.delegated_streaming_peer,
                )
                .await
                .context("request_delegated_streaming sending share audio video first error")?
                .context("request_delegated_streaming sending share audio video second error")?
            {
                Ok(response) => response,
                Err(e) => {
                    anyhow::bail!(
                        "request_delegated_streaming sending share audio video last error: {e}"
                    )
                }
            };
            Ok(streaming_key)
        } else {
            anyhow::bail!("Trying to share audio video but no active webrtc connection");
        }
    }

    pub(crate) async fn play_stream(&mut self, streaming_key: &str) -> anyhow::Result<()> {
        let peer = self
            .connection_state
            .lock()
            .await
            .as_ref()
            .map(|s| s.delegated_streaming_peer);

        if let Some(peer) = peer {
            let streaming_key: StreamingKey = streaming_key.try_into()?;
            let _ = match self
                .network
                .request_delegated_streaming(
                    DelegatedStreamingRequest::PlayStream { streaming_key },
                    peer,
                )
                .await
                .context("request_delegated_streaming sending play stream first error")?
                .context("request_delegated_streaming sending play stream second error")?
            {
                Ok(response) => response,
                Err(e) => {
                    anyhow::bail!("request_delegated_streaming sending play stream last error: {e}")
                }
            };
        } else {
            todo!()
        }
        Ok(())
    }
}

async fn really_start_playing(
    webrtc_stream: WebRtcStream,
    media_stream: MediaStream,
    media_stream_track: MediaStreamTrack,
    streaming_key: StreamingKey,
    playing_streams: PlayingStreamsType,
) {
    if let Err(e) = setup::play_video(&media_stream) {
        log::warn!("Error playing {streaming_key}: {e:?}");
    } else {
        log::info!("Received track to play {streaming_key}");
    }
    match playing_streams.lock().await.entry(streaming_key.clone()) {
        std::collections::hash_map::Entry::Occupied(mut e) => {
            e.get_mut()
                .playing_tracks
                .insert(webrtc_stream, (media_stream, media_stream_track));
        }
        std::collections::hash_map::Entry::Vacant(e) => {
            let mut playing_tracks = HashMap::new();
            playing_tracks.insert(webrtc_stream, (media_stream, media_stream_track));
            e.insert(PlayStreamState { playing_tracks });
        }
    };
}

async fn handle_delegated_streaming_event(
    info: PlayStreamNewTrackInfo,
    connection_state: ConnectionState,
) -> anyhow::Result<()> {
    let streaming_key = &info.streaming_key;
    for webrtc_track in info.tracks {
        match connection_state
            .pending_media_stream_tracks
            .lock()
            .await
            .remove(&webrtc_track.stream_id)
        {
            Some((media_stream, media_stream_track)) => {
                log::info!("Found pending media stream for {streaming_key}, will start playing");
                really_start_playing(
                    webrtc_track.stream_id,
                    media_stream,
                    media_stream_track,
                    streaming_key.clone(),
                    Arc::clone(&connection_state.playing_streams),
                )
                .await;
            }
            None => {
                log::info!("No media stream found for {streaming_key}, will save info {webrtc_track:?} for later");
                connection_state
                    .pending_webrtc_tracks
                    .lock()
                    .await
                    .insert(webrtc_track.stream_id, info.streaming_key.clone());
            }
        };
    }
    Ok(())
}
