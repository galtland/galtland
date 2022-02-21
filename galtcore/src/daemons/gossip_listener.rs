// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use libp2p::gossipsub::GossipsubMessage;
use libp2p::kad::record::Key;

use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::{KademliaRecord, RtmpStreamingRecord};
use crate::utils::{self, ArcMutex};

pub enum GossipedRtmpRecord {
    Found {
        time: SystemTime,
        record: RtmpStreamingRecord,
    },
    Searching(SystemTime),
    Missing(SystemTime),
}

#[derive(Default)]
struct State {
    recent_messages: VecDeque<GossipsubMessage>,
    found_rtmp_records: HashMap<libp2p::kad::record::Key, GossipedRtmpRecord>,
}

#[derive(Clone)]
pub struct GossipListenerClient {
    state: ArcMutex<State>,
    network: NetworkBackendClient,
}

impl GossipListenerClient {
    const MAX_RECENT_MESSAGES: usize = 5000;

    pub fn new(network: NetworkBackendClient) -> Self {
        Self {
            state: Default::default(),
            network,
        }
    }

    pub async fn whisper(&self, message: GossipsubMessage) {
        if message.topic == crate::protocols::gossip::rtmp_keys_gossip().hash() {
            let key: Key = message.data.to_vec().into();
            self.notify_rtmp_key(key).await;
        }
        let mut state = self.state.lock().await;
        state.recent_messages.push_front(message);
        state.recent_messages.truncate(Self::MAX_RECENT_MESSAGES);
    }

    pub async fn notify_rtmp_key(&self, key: Key) {
        let time = SystemTime::now();
        self.state
            .lock()
            .await
            .found_rtmp_records
            .insert(key.clone(), GossipedRtmpRecord::Searching(time));
        let state = self.state.clone();
        let mut network = self.network.clone();
        utils::spawn_and_log_error(async move {
            const MAX_RETRIES: usize = 10;
            let mut i = 0;
            loop {
                match network.get_record(key.clone()).await {
                    Ok(KademliaRecord::Rtmp(record)) => {
                        state
                            .lock()
                            .await
                            .found_rtmp_records
                            .insert(key, GossipedRtmpRecord::Found { time, record });
                        return Ok(());
                    }
                    Ok(other) => {
                        log::warn!("Received unexpected record: {other:?} while searching for key: {key:?}");
                        return Ok(());
                    }
                    Err(e) => {
                        i += 1;
                        if i <= MAX_RETRIES {
                            log::info!(
                                "Retry ({i}/{MAX_RETRIES}): received error {e:?} while searching for key: {key:?}, sleeping a little..."
                            );
                            tokio::time::sleep(Duration::from_secs(10)).await;
                        } else {
                            log::warn!("Failed to get key {key:?}, giving up");
                            let mut state = state.lock().await;
                            state
                                .found_rtmp_records
                                .insert(key, GossipedRtmpRecord::Missing(time));
                            return Ok(());
                        }
                    }
                }
            }
        });
    }

    pub async fn notify_rtmp_record(&self, record: RtmpStreamingRecord) {
        let time = SystemTime::now();
        self.state.lock().await.found_rtmp_records.insert(
            record.key.clone(),
            GossipedRtmpRecord::Found { time, record },
        );
    }

    pub async fn get_rtmp_records(&self) -> Vec<RtmpStreamingRecord> {
        self.state
            .lock()
            .await
            .found_rtmp_records
            .values()
            .filter_map(|v| match v {
                GossipedRtmpRecord::Found { record, .. } => Some(record.clone()),
                _ => None,
            })
            .collect()
    }
}
