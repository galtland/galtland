// SPDX-License-Identifier: AGPL-3.0-only


use std::collections::HashMap;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

use libp2p::PeerId;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, Mutex};

use self::modules::peer_control::PeerControl;
use self::modules::streaming::stream_publisher::StreamPublisherClient;
use crate::protocols::media_streaming::StreamingKey;
use crate::protocols::simple_file_exchange::SimpleFileResponse;
use crate::utils::ArcMutex;


pub mod modules;

#[derive(Debug)]
pub struct SentStreamingResponseStats {
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
    all_values: Vec<SentStreamingResponseStats>,
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

    fn push(&mut self, info: SentStreamingResponseStats) {
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
    pub active_streams: ArcMutex<HashMap<StreamingKey, StreamPublisherClient>>,
    pub stream_seeker: ArcMutex<HashMap<StreamingKey, oneshot::Sender<()>>>,
    pub peer_control: ArcMutex<PeerControl>,
}

impl SharedGlobalState {
    pub fn new() -> Self {
        Self {
            sent_stats: Arc::new(Mutex::new(SentStats::new())),
            active_file_transfers: Arc::new(Mutex::new(HashMap::new())),
            active_streams: Arc::new(Mutex::new(HashMap::new())),
            stream_seeker: Arc::new(Mutex::new(HashMap::new())),
            peer_control: Arc::new(Mutex::new(PeerControl::new())),
        }
    }
}

impl Default for SharedGlobalState {
    fn default() -> Self {
        Self::new()
    }
}


pub async fn feed_streaming_response_stats(
    shared_global_state: SharedGlobalState,
    info: SentStreamingResponseStats,
) {
    shared_global_state.sent_stats.lock().await.push(info);
}
