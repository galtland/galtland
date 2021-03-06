// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use libp2p::PeerId;
use tokio::sync::mpsc::{self};
use tokio::sync::{oneshot, Mutex};

use crate::cm::modules::peer_control::PeerStatistics;
use crate::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::StreamingRecord;
use crate::protocols::media_streaming::{
    StreamOffset, StreamSeekType, StreamingKey, StreamingRequest, StreamingResponse,
};
use crate::protocols::NodeIdentity;
use crate::utils::{self, spawn_and_log_error, ArcMutex};

pub(super) fn launch_daemon(
    identity: NodeIdentity,
    shared_global_state: SharedGlobalState,
    network: NetworkBackendClient,
    record: StreamingRecord,
    streaming_key: StreamingKey,
    // ) -> (tokio::task::JoinHandle<()>, PeerSeekerClient) {
) -> ((), PeerSeekerClient) {
    // TODO: perhaps receive internal network events
    let (sender, receiver) = mpsc::channel(2);
    let handle = spawn_and_log_error({
        let peer_seeker = PeerSeeker::new(
            identity,
            shared_global_state,
            network,
            record,
            streaming_key,
        );
        peer_seeker.run(receiver)
    });
    (handle, PeerSeekerClient { sender })
}

pub(super) struct PeerSeekerClient {
    sender: mpsc::Sender<SeekerCommands>,
}

impl PeerSeekerClient {
    pub(crate) async fn get_peers(&self) -> anyhow::Result<Vec<PeerId>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SeekerCommands::GetPeers { sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        receiver.await.map_err(utils::send_error)
    }

    async fn temporarily_ban(
        &self,
        peer: PeerId,
        expire_time: instant::Instant,
    ) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SeekerCommands::TemporarilyBanPeer {
                peer,
                expire_time,
                sender,
            })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        receiver.await.map_err(utils::send_error)?;
        Ok(())
    }

    pub(crate) async fn ban(&self, peer: PeerId) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SeekerCommands::PermanentlyBanPeer { peer, sender })
            .await
            .map_err(utils::send_error)?;
        tokio::task::yield_now().await;
        receiver.await.map_err(utils::send_error)?;
        Ok(())
    }

    pub(crate) async fn ban_on_recoverable_error(&self, peer: PeerId) -> anyhow::Result<()> {
        self.temporarily_ban(
            peer,
            instant::Instant::now() + PeerSeeker::BLACKLIST_DURATION_ON_RECOVERABLE_ERROR,
        )
        .await
    }

    pub(crate) async fn ban_on_flood(&self, peer: PeerId) -> anyhow::Result<()> {
        self.temporarily_ban(
            peer,
            instant::Instant::now() + PeerSeeker::BLACKLIST_DURATION_ON_FLOOD,
        )
        .await
    }

    pub(crate) async fn ban_on_max_upload(&self, peer: PeerId) -> anyhow::Result<()> {
        self.temporarily_ban(
            peer,
            instant::Instant::now() + PeerSeeker::BLACKLIST_DURATION_ON_MAX_UPLOAD,
        )
        .await
    }
}

#[derive(Debug)]
enum SeekerCommands {
    GetPeers {
        sender: oneshot::Sender<Vec<PeerId>>,
    },
    TemporarilyBanPeer {
        peer: PeerId,
        expire_time: instant::Instant,
        sender: oneshot::Sender<()>,
    },
    PermanentlyBanPeer {
        peer: PeerId,
        sender: oneshot::Sender<()>,
    },
}

enum PeerState {
    Inactive,
    Blacklisted,
    TemporarilyBlacklisted(instant::Instant),
    Empty,
    Active(StreamOffset),
}

enum PeerUpdateState {
    Idle,
    UpdatingInfo(oneshot::Sender<()>),
}

#[derive(Default)]
struct PeersSharedState {
    current_peer_statistics: HashMap<PeerId, PeerStatistics>,
    peers: HashMap<PeerId, PeerState>,
    peer_update_state: HashMap<PeerId, PeerUpdateState>,
}
struct PeerSeeker {
    identity: NodeIdentity,
    shared_global_state: SharedGlobalState,
    network: NetworkBackendClient,
    streaming_key: StreamingKey,
    record: StreamingRecord,
    peer_shared_state: ArcMutex<PeersSharedState>,
}

impl PeerSeeker {
    pub const BLACKLIST_DURATION_ON_FLOOD: Duration = Duration::from_secs(30);
    pub const BLACKLIST_DURATION_ON_GLOBAL_BAN: Duration = Duration::from_secs(60);
    pub const BLACKLIST_DURATION_ON_MAX_UPLOAD: Duration = Duration::from_secs(60);
    pub const BLACKLIST_DURATION_ON_RECOVERABLE_ERROR: Duration = Duration::from_secs(15);
    const MAX_BEST_PEERS: usize = 2;

    pub(crate) fn new(
        identity: NodeIdentity,
        shared_global_state: SharedGlobalState,
        network: NetworkBackendClient,
        record: StreamingRecord,
        streaming_key: StreamingKey,
    ) -> Self {
        Self {
            identity,
            shared_global_state,
            network,
            streaming_key,
            record,
            peer_shared_state: Arc::new(Mutex::new(Default::default())),
        }
    }

    async fn run(mut self, mut commands: mpsc::Receiver<SeekerCommands>) -> anyhow::Result<()> {
        let mut maintenance_tick = tokio::time::interval(Duration::from_secs(10));

        loop {
            log::trace!("Main loop");
            tokio::task::yield_now().await;
            tokio::select! {
                _ = maintenance_tick.tick() => {
                    if let Err(e) = self.peer_maintenance() {
                        log::warn!("Error on peer maintenance: {e}");
                    }
                },
                command = commands.recv() => {
                    match command {
                        Some(c) => self.handle_command(c).await?,
                        None => {
                            log::info!("Received empty command, exiting...");
                            break;
                        },
                    }
                }
            };
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: SeekerCommands) -> anyhow::Result<()> {
        let streaming_key = &self.streaming_key;
        match command {
            SeekerCommands::GetPeers { sender } => {
                log::debug!("SeekerCommands::GetPeers for {streaming_key:?}");
                let state = self.peer_shared_state.lock().await;

                let mut offset_candidates = state
                    .peers
                    .iter()
                    .filter_map(|(k, v)| match v {
                        PeerState::Active(offset) => Some((*offset, *k)),
                        _ => None,
                    })
                    .collect_vec();
                // reverse sorting: highest offset peers first
                offset_candidates.sort_by(|a, b| a.cmp(b).reverse());

                let final_peers = offset_candidates
                    .into_iter()
                    .map(|(_, peer)| peer)
                    .take(Self::MAX_BEST_PEERS)
                    .collect();
                sender.send(final_peers).map_err(utils::send_error)?;
            }
            SeekerCommands::TemporarilyBanPeer {
                peer,
                expire_time,
                sender,
            } => {
                log::debug!("SeekerCommands::TemporarilyBanPeer {peer} {expire_time:?} for {streaming_key:?}");
                let mut state = self.peer_shared_state.lock().await;
                Self::try_abort_update(&peer, &mut state).await;
                state
                    .peers
                    .insert(peer, PeerState::TemporarilyBlacklisted(expire_time));
                sender.send(()).map_err(utils::send_error)?;
            }
            SeekerCommands::PermanentlyBanPeer { peer, sender } => {
                let mut state = self.peer_shared_state.lock().await;
                Self::try_abort_update(&peer, &mut state).await;
                state.peers.insert(peer, PeerState::Blacklisted);
                sender.send(()).map_err(utils::send_error)?;
            }
        };
        Ok(())
    }

    async fn try_abort_update<'a>(
        peer: &PeerId,
        state: &mut tokio::sync::MutexGuard<'a, PeersSharedState>,
    ) {
        if let Some(PeerUpdateState::UpdatingInfo(sender)) = state.peer_update_state.remove(peer) {
            if sender.send(()).is_err() {
                log::warn!("update process for {peer} died before abort");
            }
        }
        state.peer_update_state.insert(*peer, PeerUpdateState::Idle);
    }

    fn peer_maintenance(&mut self) -> anyhow::Result<()> {
        spawn_and_log_error({
            let shared_global_state = self.shared_global_state.clone();
            let our_peer_id = self.identity.peer_id;
            {
                let peer_shared_state = self.peer_shared_state.clone();
                let streaming_key = self.streaming_key.clone();
                async move {
                    log::debug!(
                        "peer_maintenance update current_peer_statistics for {streaming_key:?}"
                    );
                    let peer_statistics = shared_global_state
                        .peer_control
                        .lock()
                        .await
                        .peer_statistics
                        .clone();
                    let mut state = peer_shared_state.lock().await;
                    state.current_peer_statistics.clear();
                    state.current_peer_statistics.extend(peer_statistics);
                    state.current_peer_statistics.remove(&our_peer_id);
                    Ok(())
                }
            }
        });
        spawn_and_log_error({
            let our_peer_id = self.identity.peer_id;
            let network = self.network.clone();
            let record = self.record.clone();
            let streaming_key = self.streaming_key.clone();
            let peer_shared_state = self.peer_shared_state.clone();
            let shared_global_state = self.shared_global_state.clone();
            async move {
                match network.get_providers(record.key.clone()).await {
                    Ok(Ok(mut peers)) => {
                        peers.remove(&our_peer_id);
                        log::debug!(
                            "peer_maintenance get_providers got {} peers for {streaming_key:?}",
                            peers.len()
                        );
                        let mut state = peer_shared_state.lock().await;
                        if let Some((p, stats)) = state
                            .current_peer_statistics
                            .iter()
                            .min_by_key(|(_, stats)| stats.latency)
                        {
                            if peers.insert(*p) {
                                log::debug!(
                                    "Adding fastest {p} with latency {:?} to list of peers",
                                    stats.latency
                                );
                            } else {
                                log::debug!(
                                    "Fastest {p} with latency {:?} already on list of peers",
                                    stats.latency
                                );
                            }
                        };
                        let now = instant::Instant::now();
                        for peer in peers {
                            if let PeerState::TemporarilyBlacklisted(t) =
                                state.peers.entry(peer).or_insert(PeerState::Inactive)
                            {
                                if now > *t {
                                    state.peers.insert(peer, PeerState::Inactive);
                                }
                            };
                            if let PeerUpdateState::Idle = state
                                .peer_update_state
                                .entry(peer)
                                .or_insert(PeerUpdateState::Idle)
                            {
                                if matches!(
                                    state.peers[&peer],
                                    PeerState::Active(_) | PeerState::Inactive | PeerState::Empty
                                ) {
                                    let v = {
                                        let (abort_sender, mut abort_receiver) = oneshot::channel();
                                        spawn_and_log_error({
                                            let peer_shared_state = peer_shared_state.clone();
                                            let shared_global_state = shared_global_state.clone();
                                            let network = network.clone();
                                            let streaming_key = streaming_key.clone();
                                            let record = record.clone();
                                            async move {
                                                let r = Self::update_peer(
                                                    shared_global_state,
                                                    network,
                                                    record,
                                                    streaming_key,
                                                    peer,
                                                )
                                                .await;
                                                let mut state = peer_shared_state.lock().await;
                                                if abort_receiver.try_recv().is_ok() {
                                                    log::info!(
                                                        "Received abort for peer update {peer}, exiting..."
                                                    );
                                                    return Ok(());
                                                }
                                                match r {
                                                    Ok(result) => {
                                                        state.peers.insert(peer, result);
                                                    }
                                                    Err(e) => {
                                                        log::warn!(
                                                            "Got {e:?} while updating {peer}"
                                                        )
                                                    }
                                                };
                                                state
                                                    .peer_update_state
                                                    .insert(peer, PeerUpdateState::Idle);
                                                Ok(())
                                            }
                                        });
                                        PeerUpdateState::UpdatingInfo(abort_sender)
                                    };
                                    state.peer_update_state.insert(peer, v);
                                };
                            };
                        }
                    }
                    Ok(Err(_)) => todo!(),
                    Err(_) => todo!(),
                };
                Ok(())
            }
        });
        Ok(())
    }

    async fn update_peer(
        shared_global_state: SharedGlobalState,
        network: NetworkBackendClient,
        record: StreamingRecord,
        streaming_key: StreamingKey,
        peer: PeerId,
    ) -> anyhow::Result<PeerState> {
        let result = if shared_global_state
            .peer_control
            .lock()
            .await
            .may_get_from(&peer, &streaming_key)
        {
            match network
                .request_streaming_data(
                    StreamingRequest {
                        streaming_key,
                        seek_type: StreamSeekType::Peek,
                    },
                    peer,
                )
                .await?
            {
                Ok(Ok(StreamingResponse::Data(responses))) => match responses.first() {
                    Some(r) => {
                        if r.is_valid_peek(&record) {
                            let offset = r.streaming_data.source_offset;
                            log::debug!(
                                "Got StreamingResponse::Data peek with {offset:?} from {peer}"
                            );
                            PeerState::Active(offset)
                        } else {
                            log::warn!("Got StreamingResponse::Data for peer {peer} with invalid peek signature, temporarily blacklisting...");
                            PeerState::TemporarilyBlacklisted(
                                instant::Instant::now()
                                    + Self::BLACKLIST_DURATION_ON_RECOVERABLE_ERROR,
                            )
                        }
                    }
                    None => {
                        log::debug!(
                            "Got StreamingResponse::Data peek with empty response from {peer}"
                        );
                        PeerState::Empty
                    }
                },
                Ok(Ok(StreamingResponse::MaxUploadRateReached)) => {
                    log::info!("Got StreamingResponse::MaxUploadRateReached for peer {peer}, temporarily blacklisting...");
                    PeerState::TemporarilyBlacklisted(
                        instant::Instant::now() + Self::BLACKLIST_DURATION_ON_MAX_UPLOAD,
                    )
                }
                Ok(Ok(StreamingResponse::TooMuchFlood)) => {
                    log::warn!("Got StreamingResponse::TooMuchFlood for peer {peer}, temporarily blacklisting...");
                    PeerState::TemporarilyBlacklisted(
                        instant::Instant::now() + Self::BLACKLIST_DURATION_ON_FLOOD,
                    )
                }
                Ok(Err(e)) => {
                    log::warn!("Got string error {e} for peer {peer}, temporarily blacklisting...");
                    PeerState::TemporarilyBlacklisted(
                        instant::Instant::now() + Self::BLACKLIST_DURATION_ON_RECOVERABLE_ERROR,
                    )
                }
                Err(libp2p::request_response::OutboundFailure::UnsupportedProtocols) => {
                    log::warn!(
                        "Got OutboundFailure::UnsupportedProtocols for peer {peer}, banning..."
                    );
                    PeerState::Blacklisted
                }
                Err(libp2p::request_response::OutboundFailure::DialFailure) => {
                    log::warn!(
                        "Got OutboundFailure::UnsupportedProtocols for peer {peer}, banning..."
                    );
                    PeerState::Blacklisted
                }
                Err(libp2p::request_response::OutboundFailure::ConnectionClosed) => {
                    log::warn!("Got OutboundFailure::ConnectionClosed for peer {peer}, temporarily blacklisting...");
                    PeerState::TemporarilyBlacklisted(
                        instant::Instant::now() + Self::BLACKLIST_DURATION_ON_RECOVERABLE_ERROR,
                    )
                }
                Err(libp2p::request_response::OutboundFailure::Timeout) => {
                    log::warn!(
                        "Got OutboundFailure::Timeout for peer {peer}, temporarily blacklisting..."
                    );
                    PeerState::TemporarilyBlacklisted(
                        instant::Instant::now() + Self::BLACKLIST_DURATION_ON_RECOVERABLE_ERROR,
                    )
                }
            }
        } else {
            log::warn!(
                "Peer {peer} is on global blacklist for requests, temporarily blacklisting..."
            );
            PeerState::TemporarilyBlacklisted(
                instant::Instant::now() + Self::BLACKLIST_DURATION_ON_GLOBAL_BAN,
            )
        };
        Ok(result)
    }
}
