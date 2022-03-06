// SPDX-License-Identifier: AGPL-3.0-only

pub mod payment_info;
pub mod peer_control;
pub mod rtmp;
pub mod simple_file;

use std::collections::HashMap;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use libp2p::PeerId;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, Mutex};

use self::peer_control::PeerControl;
use self::rtmp::handlers::{
    PlayRTMPStreamInfo, PublishRTMPStreamInfo, RespondPaymentInfo, RespondRTMPStreamingInfo,
};
use self::rtmp::rtmp_publisher::RtmpPublisherClient;
use self::simple_file::{GetSimpleFileInfo, PublishSimpleFileInfo, RespondSimpleFileInfo};
use crate::configuration::Configuration;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::rtmp_streaming::RtmpStreamingKey;
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
    pub now: instant::Instant,
    pub written_bytes: usize,
    pub write_duration: Duration,
    pub responses_count: u32,
}

struct SummarizedStats {
    sent_bytes: u128,
    responses: u64,
    _start_time: instant::Instant,
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
        let minimum_time = instant::Instant::now().sub(Self::RELEVANT_PERIOD);
        self.all_values.retain(|s| s.now >= minimum_time);
    }

    pub(crate) fn get_bytes_per_second(&mut self) -> u128 {
        self.compress();
        self.all_values
            .iter()
            .fold(0u128, |acc, v| acc + v.written_bytes as u128)
            / Self::RELEVANT_PERIOD.as_secs() as u128
    }
}
#[derive(Clone)]
pub struct SharedGlobalState {
    pub sent_stats: ArcMutex<SentStats>,
    pub active_file_transfers: ArcMutex<
        HashMap<(PeerId, Vec<u8>), ArcMutex<Receiver<Result<SimpleFileResponse, String>>>>,
    >,
    pub active_streams: ArcMutex<HashMap<RtmpStreamingKey, RtmpPublisherClient>>,
    // pub stream_seeker: ArcMutex<HashMap<RtmpStreamingKey, tokio::task::JoinHandle<()>>>,
    pub stream_seeker: ArcMutex<HashMap<RtmpStreamingKey, oneshot::Sender<()>>>,
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
