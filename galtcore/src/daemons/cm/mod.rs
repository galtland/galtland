// SPDX-License-Identifier: AGPL-3.0-only

pub mod payment_info;
pub mod rtmp;
pub mod simple_file;
use std::collections::{HashMap, VecDeque};
use std::ops::Sub;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use itertools::Itertools;
use libp2p::PeerId;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;

use self::rtmp::handlers::{
    PlayRTMPStreamInfo, PublishRTMPStreamInfo, RespondPaymentInfo, RespondRTMPStreamingInfo,
};
use self::rtmp::rtmp_publisher::RtmpPublisherClient;
use self::simple_file::{GetSimpleFileInfo, PublishSimpleFileInfo, RespondSimpleFileInfo};
use crate::configuration::Configuration;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::rtmp_streaming::{RTMPStreamingRequest, RtmpStreamingKey};
use crate::protocols::simple_file_exchange::SimpleFileResponse;
use crate::protocols::NodeIdentity;
use crate::utils;
use crate::utils::ArcMutex;

pub enum ClientCommand {
    RespondSimpleFile(RespondSimpleFileInfo),
    PublishSimpleFile(PublishSimpleFileInfo),
    GetSimpleFile(GetSimpleFileInfo),
    PublishRTMPStream(PublishRTMPStreamInfo),
    PlayRTMPStream(PlayRTMPStreamInfo),
    RespondRTMPStreamingRequest(RespondRTMPStreamingInfo),
    RespondPaymentInfoRequest(RespondPaymentInfo),
    FeedSentRTMPResponseStats(SentRTMPResponseStats),
}


#[derive(Debug)]
pub struct SentRTMPResponseStats {
    pub peer: PeerId,
    pub now: SystemTime,
    pub written_bytes: usize,
    pub write_duration: Duration,
    pub responses_count: u32,
}

struct SummarizedStats {
    sent_bytes: u128,
    responses: u64,
    _start_time: SystemTime,
}

pub struct SentStats {
    all_values: Vec<SentRTMPResponseStats>,
    summarized_peer_stats: HashMap<PeerId, SummarizedStats>,
}

impl SentStats {
    const MAX_ITEMS: usize = 10000;
    const RELEVANT_PERIOD: Duration = Duration::from_secs(10);

    fn new() -> Self {
        SentStats {
            all_values: Vec::with_capacity(Self::MAX_ITEMS),
            summarized_peer_stats: HashMap::new(),
        }
    }

    fn push(&mut self, info: SentRTMPResponseStats) {
        match self.summarized_peer_stats.entry(info.peer) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let s = e.get_mut();
                s.responses += info.responses_count as u64;
                s.sent_bytes += info.written_bytes as u128;
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(SummarizedStats {
                    sent_bytes: info.written_bytes as u128,
                    responses: info.responses_count as u64,
                    _start_time: info.now,
                });
            }
        }
        self.all_values.push(info);
        if self.all_values.len() >= Self::MAX_ITEMS {
            self.compress();
        }
    }

    fn compress(&mut self) {
        let minimum_time = SystemTime::now().sub(Self::RELEVANT_PERIOD);
        self.all_values.retain(|s| s.now >= minimum_time);
    }

    pub fn get_bytes_per_second(&mut self) -> u128 {
        self.compress();
        self.all_values
            .iter()
            .fold(0u128, |acc, v| acc + v.written_bytes as u128)
            / Self::RELEVANT_PERIOD.as_secs() as u128
    }
}

struct PeerStreamingRequest {
    time: SystemTime,
    request: RTMPStreamingRequest,
}

#[derive(Debug)]
#[must_use]
pub enum FloodControlResult {
    StillBlacklisted,
    Blacklisted,
    Good,
}

pub struct PeerControl {
    last_peer_requests: HashMap<PeerId, VecDeque<PeerStreamingRequest>>,
    blacklisted: HashMap<PeerId, SystemTime>,
}

impl PeerControl {
    const BLACKLIST_DURATION: Duration = Duration::from_secs(60);
    const FLOOD_MAX_REQUESTS_PER_KEY_PER_PERIOD: usize = 100;
    const FLOOD_PERIOD_OF_INTEREST: Duration = Duration::from_secs(10);
    const PROVIDE_FOR_PERIOD_OF_INTEREST: Duration = Duration::from_secs(60);

    fn new() -> Self {
        Self {
            last_peer_requests: HashMap::new(),
            blacklisted: HashMap::new(),
        }
    }

    fn blacklist(&mut self, peer: &PeerId, now: SystemTime) {
        self.last_peer_requests.remove(peer);
        self.blacklisted.insert(*peer, now);
    }

    // Returns whether peer is still blacklisted
    fn expire_blacklist(&mut self, peer: &PeerId, now: SystemTime) -> bool {
        if let Some(t) = self.blacklisted.get(peer) {
            if now.duration_since(*t).expect("time to work") > Self::BLACKLIST_DURATION {
                return true;
            }
            self.blacklisted.remove(peer);
        }
        false
    }

    pub fn flood_control(
        &mut self,
        peer: &PeerId,
        request: RTMPStreamingRequest,
    ) -> FloodControlResult {
        let now = SystemTime::now();
        if self.expire_blacklist(peer, now) {
            return FloodControlResult::StillBlacklisted;
        }
        self.last_peer_requests
            .entry(*peer)
            .or_default()
            .retain(|p| {
                now.duration_since(p.time).expect("time to work") <= Self::FLOOD_PERIOD_OF_INTEREST
            });
        let requests = &self.last_peer_requests[peer];
        if !requests.is_empty() {
            {
                let distinct_keys_len = requests
                    .iter()
                    .unique_by(|r| r.request.streaming_key.clone())
                    .count();
                let avg_requests_per_key = requests.len() / distinct_keys_len;
                if avg_requests_per_key > Self::FLOOD_MAX_REQUESTS_PER_KEY_PER_PERIOD {
                    self.blacklist(peer, now);
                    return FloodControlResult::Blacklisted;
                }
            }
            {
                let front = requests
                    .iter()
                    .find(|r| r.request.streaming_key == request.streaming_key);
                if let Some(p) = front {
                    if p.request == request {
                        self.blacklist(peer, now);
                        return FloodControlResult::Blacklisted;
                    }
                }
            }
        }
        self.last_peer_requests
            .get_mut(peer)
            .expect("already initialized")
            .push_front(PeerStreamingRequest { time: now, request });
        FloodControlResult::Good
    }

    pub fn may_get_from(&mut self, peer: &PeerId) -> bool {
        let now = SystemTime::now();
        !self.expire_blacklist(peer, now) && {
            let requests = self.last_peer_requests.get(peer);
            match requests {
                None => true,
                Some(requests) => !requests.iter().any(|r| {
                    now.duration_since(r.time).expect("time to work")
                        < Self::PROVIDE_FOR_PERIOD_OF_INTEREST
                }),
            }
        }
    }
}


#[derive(Clone)]
pub struct SharedGlobalState {
    pub sent_stats: ArcMutex<SentStats>,
    pub active_file_transfers: ArcMutex<
        HashMap<(PeerId, Vec<u8>), ArcMutex<Receiver<Result<SimpleFileResponse, String>>>>,
    >,
    pub active_streams: ArcMutex<HashMap<RtmpStreamingKey, RtmpPublisherClient>>,
    pub stream_seeker: ArcMutex<HashMap<RtmpStreamingKey, tokio::task::JoinHandle<()>>>,
    pub peer_control: ArcMutex<PeerControl>,
}

impl SharedGlobalState {
    fn new() -> Self {
        Self {
            sent_stats: Arc::new(Mutex::new(SentStats::new())),
            active_file_transfers: Arc::new(Mutex::new(HashMap::new())),
            active_streams: Arc::new(Mutex::new(HashMap::new())),
            stream_seeker: Arc::new(Mutex::new(HashMap::new())),
            peer_control: Arc::new(Mutex::new(PeerControl::new())),
        }
    }
}

pub async fn run_loop(
    opt: Configuration,
    network: NetworkBackendClient,
    mut receiver: Receiver<ClientCommand>,
    identity: NodeIdentity,
) -> anyhow::Result<()> {
    let shared_global_state = SharedGlobalState::new();
    loop {
        tokio::task::yield_now().await;
        let e = receiver.recv().await.context("Loop finished")?;
        utils::spawn_and_log_error(handle_client_command(
            opt.clone(),
            identity.clone(),
            shared_global_state.clone(),
            network.clone(),
            e,
        ));
    }
}

async fn handle_client_command(
    opt: Configuration,
    identity: NodeIdentity,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    command: ClientCommand,
) -> anyhow::Result<()> {
    match command {
        ClientCommand::PublishSimpleFile(info) => simple_file::handle_publish(network, info).await,
        ClientCommand::GetSimpleFile(info) => simple_file::handle_get(network, info).await,
        ClientCommand::RespondSimpleFile(info) => {
            simple_file::handle_respond(shared_state, network, info).await
        }
        ClientCommand::RespondPaymentInfoRequest(info) => {
            payment_info::handle_respond(network, info).await
        }
        ClientCommand::PublishRTMPStream(info) => {
            rtmp::handlers::publish(shared_state, network, info).await
        }
        ClientCommand::PlayRTMPStream(info) => {
            rtmp::handlers::play(identity, shared_state, network, info).await
        }
        ClientCommand::RespondRTMPStreamingRequest(info) => {
            rtmp::handlers::respond(opt, identity, shared_state, network, info).await
        }
        ClientCommand::FeedSentRTMPResponseStats(info) => {
            shared_state.sent_stats.lock().await.push(info);
            Ok(())
        }
    }
}
