// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

// use libp2p::gossipsub::GossipsubMessage;
use libp2p::floodsub::FloodsubMessage;
use libp2p::kad::record::Key;

use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::{KademliaRecord, StreamingRecord};
use crate::utils::{self, ArcMutex};

type SubMessage = FloodsubMessage;

#[allow(clippy::large_enum_variant)]
pub enum GossipedStreamingRecord {
    Found {
        time: instant::Instant,
        record: StreamingRecord,
    },
    Searching(instant::Instant),
    Missing(instant::Instant),
}

#[derive(Default)]
struct State {
    recent_messages: VecDeque<SubMessage>,
    found_streaming_records: HashMap<libp2p::kad::record::Key, GossipedStreamingRecord>,
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

    pub(crate) async fn whisper(&self, message: SubMessage) {
        // if message.topic == crate::protocols::gossip::streaming_keys_gossip().hash() {
        if message
            .topics
            .contains(&crate::protocols::gossip::streaming_keys_gossip())
        {
            let key: Key = message.data.to_vec().into();
            self.notify_streaming_key(key).await;
        }
        let mut state = self.state.lock().await;
        state.recent_messages.push_front(message);
        state.recent_messages.truncate(Self::MAX_RECENT_MESSAGES);
    }

    pub(crate) async fn notify_streaming_key(&self, key: Key) {
        let time = instant::Instant::now();
        self.state
            .lock()
            .await
            .found_streaming_records
            .insert(key.clone(), GossipedStreamingRecord::Searching(time));
        let state = self.state.clone();
        let network = self.network.clone();
        utils::spawn_and_log_error(async move {
            const MAX_RETRIES: usize = 10;
            let mut i = 0;
            loop {
                tokio::task::yield_now().await;
                match network.get_record(key.clone()).await {
                    Ok(KademliaRecord::MediaStreaming(record)) => {
                        state
                            .lock()
                            .await
                            .found_streaming_records
                            .insert(key, GossipedStreamingRecord::Found { time, record });
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
                                .found_streaming_records
                                .insert(key, GossipedStreamingRecord::Missing(time));
                            return Ok(());
                        }
                    }
                }
            }
        });
    }

    pub(crate) async fn notify_streaming_record(&self, record: StreamingRecord) {
        let time = instant::Instant::now();
        self.state.lock().await.found_streaming_records.insert(
            record.key.clone(),
            GossipedStreamingRecord::Found { time, record },
        );
    }

    pub async fn get_streaming_records(&self) -> Vec<StreamingRecord> {
        self.state
            .lock()
            .await
            .found_streaming_records
            .values()
            .filter_map(|v| match v {
                GossipedStreamingRecord::Found { record, .. } => Some(record.clone()),
                _ => None,
            })
            .collect()
    }
}
