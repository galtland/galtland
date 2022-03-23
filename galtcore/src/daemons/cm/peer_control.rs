use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use itertools::Itertools;
use libp2p::PeerId;

use crate::protocols::media_streaming::{StreamSeekType, StreamingRequest};


struct PeerStreamingRequest {
    time: instant::Instant,
    request: StreamingRequest,
}


#[derive(Debug)]
#[must_use]
pub enum FloodControlResult {
    StillBlacklisted,
    Blacklisted,
    Good,
}

#[derive(Default)]
pub struct PeerControl {
    last_peer_requests: HashMap<PeerId, VecDeque<PeerStreamingRequest>>,
    blacklisted: HashMap<PeerId, instant::Instant>,
}

impl PeerControl {
    const BLACKLIST_DURATION: Duration = Duration::from_secs(60);
    const FLOOD_MAX_REQUESTS_PER_KEY_PER_PERIOD: usize = 1000;
    const FLOOD_PERIOD_OF_INTEREST: Duration = Duration::from_secs(10);
    const PROVIDE_FOR_PERIOD_OF_INTEREST: Duration = Duration::from_secs(60);

    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn blacklist(&mut self, peer: &PeerId, now: instant::Instant) {
        self.last_peer_requests.remove(peer);
        self.blacklisted.insert(*peer, now);
    }

    // Returns whether peer is still blacklisted
    fn expire_blacklist(&mut self, peer: &PeerId, now: instant::Instant) -> bool {
        if let Some(t) = self.blacklisted.get(peer) {
            if now.duration_since(*t) > Self::BLACKLIST_DURATION {
                return true;
            }
            self.blacklisted.remove(peer);
        }
        false
    }

    pub(crate) fn flood_control(
        &mut self,
        peer: &PeerId,
        request: StreamingRequest,
    ) -> FloodControlResult {
        let now = instant::Instant::now();
        if self.expire_blacklist(peer, now) {
            log::debug!("{peer} still in blacklist");
            return FloodControlResult::StillBlacklisted;
        }
        self.last_peer_requests
            .entry(*peer)
            .or_default()
            .retain(|p| now.duration_since(p.time) <= Self::FLOOD_PERIOD_OF_INTEREST);
        let requests = &self.last_peer_requests[peer];
        if !requests.is_empty() {
            {
                let distinct_keys_len = requests
                    .iter()
                    .unique_by(|r| r.request.streaming_key.clone())
                    .count();
                let requests_len = requests.len();
                let avg_requests_per_key = requests_len / distinct_keys_len;
                let max = Self::FLOOD_MAX_REQUESTS_PER_KEY_PER_PERIOD;
                if avg_requests_per_key > max {
                    log::debug!("Blacklisting {peer} because avg_requests_per_key {avg_requests_per_key} (= {requests_len} / {distinct_keys_len}) > {max} ");
                    self.blacklist(peer, now);
                    return FloodControlResult::Blacklisted;
                }
            }
            {
                let front = requests
                    .iter()
                    .find(|r| r.request.streaming_key == request.streaming_key);
                if let Some(p) = front {
                    if p.request.seek_type != StreamSeekType::Peek && p.request == request {
                        log::debug!("Blacklisting {peer} because request is repeated: {request:?}");
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

    pub(crate) fn may_get_from(&mut self, peer: &PeerId) -> bool {
        let now = instant::Instant::now();
        !self.expire_blacklist(peer, now) && {
            let requests = self.last_peer_requests.get(peer);
            match requests {
                None => true,
                Some(requests) => !requests
                    .iter()
                    .any(|r| now.duration_since(r.time) < Self::PROVIDE_FOR_PERIOD_OF_INTEREST),
            }
        }
    }
}
