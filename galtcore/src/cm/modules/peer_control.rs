use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use itertools::Itertools;
use libp2p::{Multiaddr, PeerId};

use crate::protocols::galt_identify::{GaltOrganization, KnownPeer, KnownPeerAddresses};
use crate::protocols::media_streaming::{StreamSeekType, StreamingKey, StreamingRequest};


#[derive(Clone, Default)]
pub struct PeerStatistics {
    pub latency: Option<Duration>,
    pub external_addresses: Vec<Multiaddr>,
    pub org: Option<GaltOrganization>,
}

impl std::fmt::Debug for PeerStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Latency: {:?}, org: {:?}, external addresses: {:?}",
            self.latency, self.org, self.external_addresses
        ))
    }
}


impl std::fmt::Display for PeerStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Latency: {:?}, org: {:?}, external addresses count: {:?}",
            self.latency,
            self.org,
            self.external_addresses.len()
        ))
    }
}

#[derive(Clone)]
pub struct PeerStreamingRequest {
    time: instant::Instant,
    request: StreamingRequest,
}


#[derive(Debug)]
#[must_use]
pub enum FloodControlResult {
    StillBlacklisted,
    Blacklisted,
    Good,
    SameOrgPeer,
}

type KnownPeersMap = HashMap<PeerId, KnownPeerAddresses>;

#[derive(Default)]
pub struct PeerControl {
    pub last_peer_requests: HashMap<PeerId, VecDeque<PeerStreamingRequest>>,
    pub blacklisted: HashMap<PeerId, instant::Instant>,
    pub other_orgs_peers: HashMap<GaltOrganization, KnownPeersMap>,
    pub our_org_peers: KnownPeersMap,
    pub peer_statistics: HashMap<PeerId, PeerStatistics>,
    // TODO: create statistics for orgs (upload/download/payments)
}

impl PeerControl {
    const BLACKLIST_DURATION: Duration = Duration::from_secs(60);
    const FLOOD_MAX_REQUESTS_PER_KEY_PER_PERIOD: usize = 100000;
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
        if self.our_org_peers.contains_key(peer) {
            return FloodControlResult::SameOrgPeer;
        }
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

    pub(crate) fn may_get_from(&mut self, peer: &PeerId, streaming_key: &StreamingKey) -> bool {
        let now = instant::Instant::now();
        let allowed = !self.expire_blacklist(peer, now) && {
            let requests = self.last_peer_requests.get(peer);
            match requests {
                None => true,
                Some(requests) => !requests.iter().any(|r| {
                    now.duration_since(r.time) < Self::PROVIDE_FOR_PERIOD_OF_INTEREST
                        && r.request.streaming_key == *streaming_key
                }),
            }
        };

        allowed
    }

    pub(crate) fn add_our_org_peer(&mut self, k: KnownPeer) {
        self.our_org_peers.insert(k.peer, k.external_addresses);
    }

    pub(crate) fn add_org_peers(
        &mut self,
        known_peers: impl Iterator<Item = KnownPeer>,
        org: GaltOrganization,
    ) {
        self.other_orgs_peers
            .entry(org)
            .or_default()
            .extend(known_peers.map(|k| (k.peer, k.external_addresses)));
    }

    pub(crate) fn add_ping_info(&mut self, p: PeerId, rtt: Option<Duration>) {
        self.peer_statistics.entry(p).or_default().latency = rtt;
    }

    pub(crate) fn add_external_address(&mut self, p: PeerId, external_addresses: Vec<Multiaddr>) {
        self.peer_statistics
            .entry(p)
            .or_default()
            .external_addresses = external_addresses;
    }

    pub(crate) fn add_peer_org(&mut self, p: PeerId, org: GaltOrganization) {
        self.peer_statistics.entry(p).or_default().org = Some(org);
    }
}
