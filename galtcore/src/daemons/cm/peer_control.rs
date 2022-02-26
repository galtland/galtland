use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use itertools::Itertools;
use libp2p::PeerId;

use crate::protocols::rtmp_streaming::RTMPStreamingRequest;


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

    pub fn new() -> Self {
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
            log::debug!("{peer} still in blacklist");
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
                let requests_len = requests.len();
                let avg_requests_per_key = requests_len / distinct_keys_len;
                let max = Self::FLOOD_MAX_REQUESTS_PER_KEY_PER_PERIOD;
                if avg_requests_per_key > max {
                    log::debug!("Blacklisting {peer} because {avg_requests_per_key} (= {requests_len} / {distinct_keys_len}) > {max} ");
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
