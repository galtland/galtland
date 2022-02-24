// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;

use itertools::Itertools;
use libp2p::PeerId;
use log::debug;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;

use crate::daemons::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::{KademliaRecord, RtmpStreamingRecord};
use crate::protocols::rtmp_streaming::{
    self, RTMPDataSeekType, RTMPFrameType, RTMPStreamingResponseResult, RtmpStreamingKey,
    RtmpStreamingResponse, SignedRtmpData,
};
use crate::{protocols, utils};

type SenderRtmpData = mpsc::Sender<RTMPDataClientCommand>;

#[derive(Debug)]
enum RTMPDataClientCommand {
    NewRTMPData {
        rtmp_data: Vec<SignedRtmpData>,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    GetRTMPData {
        peer: PeerId,
        seek_type: RTMPDataSeekType,
        sender: oneshot::Sender<RTMPStreamingResponseResult>,
    },
    GetLastSent {
        sender: oneshot::Sender<SystemTime>,
    },
    Die {
        sender: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub struct RtmpPublisherClient {
    sender: SenderRtmpData,
}

impl RtmpPublisherClient {
    pub async fn feed_data(&self, rtmp_data: Vec<SignedRtmpData>) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(RTMPDataClientCommand::NewRTMPData { rtmp_data, sender })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn get_data(
        &self,
        peer: PeerId,
        seek_type: RTMPDataSeekType,
    ) -> anyhow::Result<RTMPStreamingResponseResult> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(RTMPDataClientCommand::GetRTMPData {
                peer,
                seek_type,
                sender,
            })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn get_last_sent(&self) -> anyhow::Result<SystemTime> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(RTMPDataClientCommand::GetLastSent { sender })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn die(&self) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(RTMPDataClientCommand::Die { sender })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }
}

async fn _rtmp_publisher_daemon(
    streaming_key: RtmpStreamingKey,
    mut network: NetworkBackendClient,
    mut command_receiver: mpsc::Receiver<RTMPDataClientCommand>,
    record: RtmpStreamingRecord,
) -> anyhow::Result<()> {
    const MAX_BUFFER_RESPONSES: usize = 5000;
    // Note: responses here are saved in the "reverse order" compare to other lists in other places
    // This means that the left/head is the newest data and the right/tail is the oldest
    // We do that to enable truncate to remove old items from the right/tail end
    let mut video_header: Option<Arc<SignedRtmpData>> = None;
    let mut audio_header: Option<Arc<SignedRtmpData>> = None;
    let mut last_video_keyframe: Option<Arc<SignedRtmpData>> = None;
    let mut last_audio: Option<Arc<SignedRtmpData>> = None;
    let mut consumers: HashMap<PeerId, VecDeque<Arc<SignedRtmpData>>> = HashMap::new();

    let mut last_sent = SystemTime::now();
    let mut published = false;

    loop {
        tokio::task::yield_now().await;
        match command_receiver.recv().await {
            Some(RTMPDataClientCommand::NewRTMPData { rtmp_data, sender })
                if rtmp_data.is_empty() =>
            {
                log::debug!("Empty rtmp_data received");
                sender.send(Ok(())).map_err(utils::send_error)?;
                tokio::task::yield_now().await;
            }

            Some(RTMPDataClientCommand::NewRTMPData { rtmp_data, sender }) => {
                let mut failure_processing_responses = false;
                let new_responses = rtmp_data
                    .into_iter()
                    .map(|r| {
                        let frame_type = RTMPFrameType::frame_type(&r.rtmp_data.data);
                        let r = Arc::new(r);
                        match frame_type {
                            RTMPFrameType::VideoSequenceHeader => {
                                log::info!(
                                    "Received video sequence header for {:?}",
                                    streaming_key
                                );
                                video_header.replace(Arc::clone(&r));
                                last_video_keyframe.take();
                            }
                            RTMPFrameType::VideoKeyframe => {
                                last_video_keyframe.replace(Arc::clone(&r));
                            }
                            RTMPFrameType::AudioSequenceHeader => {
                                log::info!(
                                    "Received audio sequence header for {:?}",
                                    streaming_key
                                );
                                audio_header.replace(Arc::clone(&r));
                                last_audio.take();
                            }
                            RTMPFrameType::KeyAudio => {
                                last_audio.replace(Arc::clone(&r));
                            }
                            RTMPFrameType::Invalid => failure_processing_responses = true,
                            RTMPFrameType::Other => {}
                        };
                        r
                    })
                    .collect_vec();

                if failure_processing_responses {
                    sender
                        .send(Err(anyhow::anyhow!("Invalid frame received, aborting...")))
                        .map_err(utils::send_error)?;
                    tokio::task::yield_now().await;
                    continue;
                }

                let mut consumers_to_remove = HashSet::new();
                for (k, peer_responses) in &mut consumers {
                    if peer_responses.len() + new_responses.len() > MAX_BUFFER_RESPONSES {
                        log::info!(
                            "Removing {} as a peer to publish because will overflow queue",
                            k
                        );
                        consumers_to_remove.insert(*k); // TODO: perhaps avoid a complete reset?
                    } else {
                        for response in &new_responses {
                            peer_responses.push_back(Arc::clone(response));
                        }
                    }
                }
                for consumer_to_remove in &consumers_to_remove {
                    consumers.remove(consumer_to_remove);
                }
                sender.send(Ok(())).map_err(utils::send_error)?;
                tokio::task::yield_now().await;
                if !published {
                    match publisher_publish(&streaming_key, &mut network, &record).await {
                        Ok(_) => published = true,
                        Err(e) => log::warn!("Failed to publish {streaming_key:?}: {e:?}"),
                    };
                }
            }

            Some(RTMPDataClientCommand::GetRTMPData {
                peer,
                sender,
                seek_type: RTMPDataSeekType::Reset,
            }) => {
                consumers.remove(&peer);
                let to_send = [
                    &video_header,
                    &audio_header,
                    &last_audio,
                    &last_video_keyframe,
                ]
                .into_iter()
                .flatten()
                .map(|r| r.as_ref().clone())
                .sorted_by_key(|r| r.rtmp_data.source_offset)
                .collect_vec();
                log::info!(
                    "Initializing {} responses for {} RTMPDataSeekType::Reset",
                    to_send.len(),
                    peer,
                );
                if !to_send.is_empty() {
                    last_sent = SystemTime::now();
                }
                if sender
                    .send(Ok(RtmpStreamingResponse::Data(to_send)))
                    .is_err()
                {
                    log::warn!(
                        "Requester of {:?} for peer {} died, resetting responses...",
                        streaming_key,
                        peer
                    );
                } else {
                    consumers.insert(peer, Default::default());
                }
            }

            Some(RTMPDataClientCommand::GetRTMPData {
                peer,
                sender,
                seek_type: RTMPDataSeekType::Offset(n),
            }) => {
                let base_data = [
                    &video_header,
                    &audio_header,
                    &last_audio,
                    &last_video_keyframe,
                ];
                let peer_responses = consumers.entry(peer).or_insert_with(|| {
                    let new_responses: VecDeque<Arc<SignedRtmpData>> = base_data
                        .into_iter()
                        .flatten()
                        .cloned()
                        .sorted_by_key(|r| r.rtmp_data.source_offset)
                        .collect();
                    log::info!(
                        "Initializing {} responses for {} RTMPDataSeekType::Offset({n:?})",
                        new_responses.len(),
                        peer,
                    );
                    new_responses
                });
                // remove old data
                while let Some(p) = peer_responses.pop_front() {
                    if p.rtmp_data.source_offset > n {
                        peer_responses.push_front(p); // this is new data, put back on responses
                        break;
                    }
                }
                // get new data
                let to_send = peer_responses
                    .iter_mut()
                    .filter(|r| r.rtmp_data.source_offset > n)
                    .map(|r| r.as_ref().clone())
                    .take(rtmp_streaming::MAX_RESPONSES_PER_REQUEST)
                    .collect_vec();
                if !to_send.is_empty() {
                    last_sent = SystemTime::now();
                }
                if sender
                    .send(Ok(RtmpStreamingResponse::Data(to_send)))
                    .is_err()
                {
                    log::warn!(
                        "Requester of {:?} for peer {} died, resetting responses...",
                        streaming_key,
                        peer
                    );
                    consumers.remove(&peer);
                }
            }

            Some(RTMPDataClientCommand::GetRTMPData {
                peer,
                sender,
                seek_type: RTMPDataSeekType::Peek,
            }) => {
                let most_recent_frame = [
                    &video_header,
                    &audio_header,
                    &last_audio,
                    &last_video_keyframe,
                ]
                .into_iter()
                .flatten()
                .max_by_key(|r| r.rtmp_data.source_offset)
                .map(|r| r.as_ref().clone());
                let to_send = most_recent_frame.into_iter().collect_vec();
                if sender
                    .send(Ok(RtmpStreamingResponse::Data(to_send)))
                    .is_err()
                {
                    log::warn!("Requester of {:?} for peer {} died,", streaming_key, peer);
                }
            }
            Some(RTMPDataClientCommand::GetLastSent { sender }) => {
                if sender.send(last_sent).is_err() {
                    log::warn!("Requester of GetLastSent {:?} died", streaming_key);
                };
            }
            Some(RTMPDataClientCommand::Die { sender }) => {
                if let Err(e) = publisher_unpublish(&mut network, &record).await {
                    log::warn!("Error unpublishing data for {streaming_key:?}: {e:?}");
                } else {
                    log::info!("Cleanly exiting {streaming_key:?}");
                }
                if sender.send(()).is_err() {
                    log::warn!("Requester of {:?} died,", streaming_key);
                }
                return Ok(()); // exit
            }
            None => {
                todo!()
            }
        };
    }
}

async fn publisher_unpublish(
    network: &mut NetworkBackendClient,
    record: &RtmpStreamingRecord,
) -> anyhow::Result<()> {
    let key = &record.key;
    network.remove_record(key.clone()).await?;
    network.stop_providing(key.clone()).await
}

async fn publisher_publish(
    streaming_key: &RtmpStreamingKey,
    network: &mut NetworkBackendClient,
    record: &RtmpStreamingRecord,
) -> anyhow::Result<()> {
    debug!("rtmp_publisher_daemon initialization {:?}", streaming_key);
    let kad_record = KademliaRecord::Rtmp(record.clone());
    let kad_key = kad_record.key().clone();
    network.put_record(kad_record).await?;
    network
        .start_providing_rtmp_streaming(kad_key.clone(), streaming_key.clone())
        .await?;
    network
        .publish_gossip(kad_key.to_vec(), protocols::gossip::rtmp_keys_gossip())
        .await?;
    Ok(())
}

pub async fn launch_daemon(
    streaming_key: RtmpStreamingKey,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    initializer: oneshot::Sender<anyhow::Result<RtmpPublisherClient>>,
    record: RtmpStreamingRecord,
) -> anyhow::Result<RtmpPublisherClient> {
    log::debug!("Launching rtmp daemon for {:?}", &streaming_key);
    let (command_sender, command_receiver) = tokio::sync::mpsc::channel(2);
    let client = match shared_state
        .active_streams
        .lock()
        .await
        .entry(streaming_key.clone())
    {
        std::collections::hash_map::Entry::Occupied(_) => {
            match initializer.send(Err(anyhow::anyhow!(
                "While was launching rtmp publisher daemon already found a active stream for {:?}",
                streaming_key
            ))) {
                Ok(_) => {}
                Err(_) => {
                    log::warn!(
                        "Error telling initializer that rtmp publisher daemon failed for {:?}",
                        streaming_key
                    );
                }
            };
            anyhow::bail!(
                "While was launching rtmp publisher daemon already found a active stream for {:?}",
                streaming_key
            );
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            let client = RtmpPublisherClient {
                sender: command_sender,
            };
            entry.insert(client.clone());
            if initializer.send(Ok(client.clone())).is_err() {
                log::warn!(
                    "Error telling initializer that rtmp publisher daemon succeed for {:?}",
                    streaming_key,
                );
            };
            client
        }
    };
    utils::spawn_and_log_error({
        let client = client.clone();
        async move {
            let r = _rtmp_publisher_daemon(streaming_key, network, command_receiver, record).await;
            // Remove ourselves
            shared_state
                .active_streams
                .lock()
                .await
                .retain(|_, v| !client.sender.same_channel(&v.sender));
            r
        }
    });
    Ok(client)
}
