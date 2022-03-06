use std::collections::HashMap;
use std::rc::Rc;

use anyhow::Context;
use galtcore::daemons::internal_network_events::BroadcastableNetworkEvent;
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::webrtc_signaling::{self, SignalingRequest, SignalingResponse};
use galtcore::tokio::sync::{broadcast, mpsc};
use galtcore::utils::ArcMutex;
use galtcore::{rmp_serde, tokio};
use libp2p::PeerId;
use web_sys::RtcPeerConnection;

use super::setup::{self, MyRTCIceCandidateInit};

type ConnectionsType = ArcMutex<HashMap<PeerId, Rc<RtcPeerConnection>>>;
#[derive(Default, Clone)]
pub struct WebRtcState {
    connections: ConnectionsType,
}

impl WebRtcState {
    pub(crate) async fn run_receive_loop(
        self,
        mut network_event_receiver: broadcast::Receiver<BroadcastableNetworkEvent>,
        mut webrtc_request_event_receiver: mpsc::UnboundedReceiver<webrtc_signaling::RequestEvent>,
        network: NetworkBackendClient,
    ) -> anyhow::Result<()> {
        loop {
            log::debug!("Looping receive loop");
            tokio::select! {
                event = network_event_receiver.recv() => {
                    match event.context("receiving broadcast")? {
                        BroadcastableNetworkEvent::ReceivedGossip { .. } => {}
                        BroadcastableNetworkEvent::PutRecord { .. } => {}
                        BroadcastableNetworkEvent::SentRTMPResponseStats { .. } => {}
                        BroadcastableNetworkEvent::ConnectionEstablished { peer } => {
                            if !self.connections.lock().await.contains_key(&peer) {
                                log::info!("Handling connection established for {peer}");
                                let connections = self.connections.clone();
                                let network = network.clone();
                                galtcore::utils::wspawn_and_log_error(async move {
                                    Self::handle_connection_established(connections, peer, network).await?;
                                    Ok(())
                                });
                            } else {
                                log::info!("Reconnecting to {peer} but webrtc connection has been already established");
                            }
                        }
                    };
                },
                event = webrtc_request_event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            let network = network.clone();
                            let connection = self.connections.lock().await.get_mut(&event.peer).cloned();
                            match connection {
                                Some(connection) => {
                                    log::info!(
                                        "Handling event request from {} with {} ice candidates",
                                        event.peer,
                                        event.request.ice_candidates.len()
                                    );
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
                }
            }
        }
    }

    async fn handle_connection_established(
        connections: ConnectionsType,
        peer: PeerId,
        network: NetworkBackendClient,
    ) -> anyhow::Result<()> {
        let (connection, mut ice_candidates_receiver, mut on_negotiation_needed_receiver) =
            setup::webrtc_initialize();
        match connections.lock().await.entry(peer) {
            std::collections::hash_map::Entry::Occupied(_) => {
                anyhow::bail!("Some concurrency issue while initializing webrtc for {peer}")
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(connection.clone());
            }
        }
        galtcore::utils::wspawn_and_log_error({
            let mut network = network.clone();
            async move {
                Self::create_offer(peer, &connection, &mut network).await?;

                while (on_negotiation_needed_receiver.recv().await).is_some() {
                    Self::create_offer(peer, &connection, &mut network).await?;
                }
                log::info!("Exiting from on negotiation needed receiver for {peer}...");
                Ok(())
            }
        });
        galtcore::utils::wspawn_and_log_error({
            let mut network = network.clone();
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
                    log::info!("Sending {} ice candidates to {peer}", ice_candidates.len());
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
                    };
                }
                log::info!("Exiting from ice candidate receiver from {peer}...");
                Ok(())
            }
        });
        Ok(())
    }

    async fn handle_request_event(
        event: webrtc_signaling::RequestEvent,
        connection: Rc<RtcPeerConnection>,
        mut network: NetworkBackendClient,
    ) -> anyhow::Result<()> {
        if !event.request.ice_candidates.is_empty() {
            for candidate in &event.request.ice_candidates {
                let my_ice_candidate = match candidate {
                    Some(candidate) => {
                        let my_ice_candidate: MyRTCIceCandidateInit =
                            rmp_serde::from_slice(candidate)
                                .context("deserializing ice candidate init")?;
                        Some(my_ice_candidate)
                    }
                    None => None,
                };
                setup::add_ice_candidate(&connection, my_ice_candidate).await?;
            }
            network
                .respond_webrtc_signaling(
                    Ok(SignalingResponse {
                        answer: None,
                        ice_candidates: Vec::new(),
                    }),
                    event.channel,
                )
                .await?;
        } else {
            let message = format!(
                "Received no candidates on ice request from {}: has offer? {}",
                event.peer,
                event.request.offer.is_some()
            );
            log::warn!("{}", message);
            network
                .respond_webrtc_signaling(Err(message), event.channel)
                .await?;
        }
        Ok(())
    }

    fn serialize_ice_candidate(c: &MyRTCIceCandidateInit) -> anyhow::Result<Vec<u8>> {
        rmp_serde::to_vec(c).context("serializing rtc ice candidate")
    }

    async fn create_offer(
        peer: PeerId,
        connection: &Rc<RtcPeerConnection>,
        network: &mut NetworkBackendClient,
    ) -> anyhow::Result<()> {
        let session_description = setup::create_offer(connection).await?;
        let offer =
            rmp_serde::to_vec(&session_description).context("serializing session description")?;
        let answer = match network
            .request_webrtc_signaling(
                SignalingRequest {
                    offer: Some(offer),
                    ice_candidates: Default::default(),
                },
                peer,
            )
            .await
            .context("request_webrtc_signaling sending offer first error")?
            .context("request_webrtc_signaling sending offer second error")?
        {
            Ok(r) => r.answer,
            Err(e) => {
                anyhow::bail!("request_webrtc_signaling sending offer last error: {e}")
            }
        };

        let answer: setup::MySessionDescription = rmp_serde::from_slice(
            &answer.ok_or_else(|| anyhow::anyhow!("received none as answer"))?,
        )
        .context("deserializing session description")?;
        setup::receive_answer(connection, &answer).await?;

        Ok(())
    }

    pub(crate) async fn share_screen(&self) -> anyhow::Result<()> {
        let display_media = setup::get_display_media().await?;
        for (_peer, connection) in self.connections.lock().await.iter_mut() {
            setup::share_screen(&display_media, connection).await?;
        }
        Ok(())
    }
}
