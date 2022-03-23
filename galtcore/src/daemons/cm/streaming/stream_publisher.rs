// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
// use libp2p::gossipsub::error::PublishError;
use libp2p::PeerId;
use log::debug;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;

use crate::daemons::cm::SharedGlobalState;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::kademlia_record::{KademliaRecord, StreamingRecord};
use crate::protocols::media_streaming::{
    self, RTMPFrameType, SignedStreamingData, StreamSeekType, StreamingDataType, StreamingKey,
    StreamingResponse, StreamingResponseResult,
};
use crate::{protocols, utils};

type SenderStreamingData = mpsc::Sender<StreamingDataClientCommand>;

#[derive(Debug)]
enum StreamingDataClientCommand {
    NewStreamingData {
        data: Vec<SignedStreamingData>,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    GetStreamingData {
        peer: PeerId,
        seek_type: StreamSeekType,
        sender: oneshot::Sender<StreamingResponseResult>,
    },
    GetLastSent {
        sender: oneshot::Sender<instant::Instant>,
    },
    Die {
        sender: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub struct StreamPublisherClient {
    sender: SenderStreamingData,
}

impl StreamPublisherClient {
    pub async fn feed_data(&self, data: Vec<SignedStreamingData>) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(StreamingDataClientCommand::NewStreamingData { data, sender })
            .await?;
        tokio::task::yield_now().await;
        receiver.await?
    }

    pub async fn get_data(
        &self,
        peer: PeerId,
        seek_type: StreamSeekType,
    ) -> anyhow::Result<StreamingResponseResult> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(StreamingDataClientCommand::GetStreamingData {
                peer,
                seek_type,
                sender,
            })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn get_last_sent(&self) -> anyhow::Result<instant::Instant> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(StreamingDataClientCommand::GetLastSent { sender })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }

    pub async fn die(&self) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(StreamingDataClientCommand::Die { sender })
            .await?;
        tokio::task::yield_now().await;
        Ok(receiver.await?)
    }
}

async fn _stream_publisher_daemon(
    streaming_key: StreamingKey,
    mut network: NetworkBackendClient,
    mut command_receiver: mpsc::Receiver<StreamingDataClientCommand>,
    record: StreamingRecord,
) -> anyhow::Result<()> {
    const MAX_BUFFER_RESPONSES: usize = 10000;
    // Note: responses here are saved in the "reverse order" compare to other lists in other places
    // This means that the left/head is the newest data and the right/tail is the oldest
    // We do that to enable truncate to remove old items from the right/tail end
    let mut video_header: Option<Arc<SignedStreamingData>> = None;
    let mut audio_header: Option<Arc<SignedStreamingData>> = None;
    let mut last_video_keyframe: Option<Arc<SignedStreamingData>> = None;
    let mut last_video: Option<Arc<SignedStreamingData>> = None;
    let mut last_audio: Option<Arc<SignedStreamingData>> = None;
    let mut consumers: HashMap<PeerId, VecDeque<Arc<SignedStreamingData>>> = HashMap::new();

    let mut last_sent = instant::Instant::now();
    let mut published = false;

    loop {
        tokio::task::yield_now().await;
        match command_receiver.recv().await {
            Some(StreamingDataClientCommand::NewStreamingData { data, sender })
                if data.is_empty() =>
            {
                log::debug!("StreamingDataClientCommand::NewStreamingData empty data received");
                sender.send(Ok(())).map_err(utils::send_error)?;
                tokio::task::yield_now().await;
            }

            Some(StreamingDataClientCommand::NewStreamingData { data, sender }) => {
                let mut failure_processing_responses = false;
                let new_responses = data
                    .into_iter()
                    .map(|r| match r.streaming_data.data_type {
                        StreamingDataType::RtmpAudio | StreamingDataType::RtmpVideo => {
                            let frame_type = RTMPFrameType::classify(&r.streaming_data.data);
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
                                    log::debug!("Received RTMPFrameType::VideoKeyframe");
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
                                RTMPFrameType::Video => {
                                    log::trace!("Received RTMPFrameType::Video");
                                    last_video.replace(Arc::clone(&r));
                                }
                                RTMPFrameType::KeyAudio => {
                                    log::trace!("Received RTMPFrameType::KeyAudio");
                                    last_audio.replace(Arc::clone(&r));
                                }
                                RTMPFrameType::Invalid => {
                                    log::warn!("Received RTMPFrameType::Invalid");
                                    failure_processing_responses = true;
                                }
                                RTMPFrameType::Other => {
                                    log::info!("Received RTMPFrameType::Other");
                                }
                            };
                            r
                        }
                        StreamingDataType::WebRtcRtpPacket => {
                            let r = Arc::new(r);
                            last_video.replace(Arc::clone(&r));
                            r
                        }
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
                        log::info!("Removing {k} as a peer to publish because will overflow queue");
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

            Some(StreamingDataClientCommand::GetStreamingData {
                peer,
                sender,
                seek_type: StreamSeekType::Reset,
            }) => {
                consumers.remove(&peer);
                let to_send = [
                    &video_header,
                    &audio_header,
                    &last_video_keyframe,
                    &last_video,
                    &last_audio,
                ]
                .into_iter()
                .flatten()
                .map(|r| r.as_ref().clone())
                .sorted_by_key(|r| r.streaming_data.source_offset)
                .collect_vec();
                log::info!(
                    "Initializing {} responses for {} StreamSeekType::Reset",
                    to_send.len(),
                    peer,
                );
                if !to_send.is_empty() {
                    last_sent = instant::Instant::now();
                }
                if sender.send(Ok(StreamingResponse::Data(to_send))).is_err() {
                    log::warn!(
                        "Requester of {:?} for peer {} died, resetting responses...",
                        streaming_key,
                        peer
                    );
                } else {
                    consumers.insert(peer, Default::default());
                }
            }

            Some(StreamingDataClientCommand::GetStreamingData {
                peer,
                sender,
                seek_type: StreamSeekType::Offset(n),
            }) => {
                let base_data = [
                    &video_header,
                    &audio_header,
                    &last_video_keyframe,
                    &last_video,
                    &last_audio,
                ];
                let peer_responses = consumers.entry(peer).or_insert_with(|| {
                    let new_responses: VecDeque<Arc<SignedStreamingData>> = base_data
                        .into_iter()
                        .flatten()
                        .cloned()
                        .sorted_by_key(|r| r.streaming_data.source_offset)
                        .collect();
                    log::info!(
                        "Initializing {} responses for {} StreamSeekType::Offset({n:?})",
                        new_responses.len(),
                        peer,
                    );
                    new_responses
                });
                // remove old data
                while let Some(p) = peer_responses.pop_front() {
                    if p.streaming_data.source_offset > n {
                        peer_responses.push_front(p); // this is new data, put back on responses
                        break;
                    }
                }
                // get new data
                let to_send = peer_responses
                    .iter_mut()
                    .filter(|r| r.streaming_data.source_offset > n)
                    .map(|r| r.as_ref().clone())
                    .take(media_streaming::MAX_RESPONSES_PER_REQUEST)
                    .collect_vec();
                if !to_send.is_empty() {
                    last_sent = instant::Instant::now();
                }
                if sender.send(Ok(StreamingResponse::Data(to_send))).is_err() {
                    log::warn!(
                        "Requester of {:?} for peer {} died, resetting responses...",
                        streaming_key,
                        peer
                    );
                    consumers.remove(&peer);
                }
            }

            Some(StreamingDataClientCommand::GetStreamingData {
                peer,
                sender,
                seek_type: StreamSeekType::Peek,
            }) => {
                let most_recent_frame = [
                    &video_header,
                    &audio_header,
                    &last_video_keyframe,
                    &last_video,
                    &last_audio,
                ]
                .into_iter()
                .flatten()
                .max_by_key(|r| r.streaming_data.source_offset)
                .map(|r| r.as_ref().clone());
                let to_send = most_recent_frame.into_iter().collect_vec();
                log::debug!("StreamingDataClientCommand::GetStreamingData seek_type: StreamSeekType::Peek returning {} responses", to_send.len());
                if sender.send(Ok(StreamingResponse::Data(to_send))).is_err() {
                    log::warn!("Requester of {:?} for peer {} died,", streaming_key, peer);
                }
            }
            Some(StreamingDataClientCommand::GetLastSent { sender }) => {
                if sender.send(last_sent).is_err() {
                    log::warn!("Requester of GetLastSent {:?} died", streaming_key);
                };
            }
            Some(StreamingDataClientCommand::Die { sender }) => {
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
    record: &StreamingRecord,
) -> anyhow::Result<()> {
    let key = &record.key;
    network.remove_record(key.clone()).await?;
    network.stop_providing(key.clone()).await
}

async fn publisher_publish(
    streaming_key: &StreamingKey,
    network: &mut NetworkBackendClient,
    record: &StreamingRecord,
) -> anyhow::Result<()> {
    debug!("publisher_publish initialization {:?}", streaming_key);
    let kad_record = KademliaRecord::MediaStreaming(record.clone());
    let kad_key = kad_record.key().clone();
    network.put_record(kad_record).await?;
    network
        .start_providing_streaming(kad_key.clone(), streaming_key.clone())
        .await
        .context("publisher_publish start_providing_streaming first error")?
        .context("publisher_publish start_providing_streaming second error")?;
    // match network
    //     .publish_gossip(kad_key.to_vec(), protocols::gossip::streaming_keys_gossip())
    //     .await?
    // {
    //     Ok(_) => {}
    //     Err(PublishError::Duplicate) => {} // it's okay
    //     Err(e) => anyhow::bail!("Publish error: {e:?}"),
    // };
    network
        .publish_gossip(kad_key.to_vec(), protocols::gossip::streaming_keys_gossip())
        .await?;

    Ok(())
}

pub(crate) async fn launch_daemon(
    streaming_key: StreamingKey,
    shared_state: SharedGlobalState,
    network: NetworkBackendClient,
    initializer: oneshot::Sender<anyhow::Result<StreamPublisherClient>>,
    record: StreamingRecord,
) -> anyhow::Result<StreamPublisherClient> {
    log::debug!("Launching stream daemon for {:?}", &streaming_key);
    let (command_sender, command_receiver) = tokio::sync::mpsc::channel(2);
    let client = match shared_state
        .active_streams
        .lock()
        .await
        .entry(streaming_key.clone())
    {
        std::collections::hash_map::Entry::Occupied(_) => {
            match initializer.send(Err(anyhow::anyhow!(
                "While was launching stream publisher daemon already found a active stream for {:?}",
                streaming_key
            ))) {
                Ok(_) => {}
                Err(_) => {
                    log::warn!(
                        "Error telling initializer that stream publisher daemon failed for {:?}",
                        streaming_key
                    );
                }
            };
            anyhow::bail!(
                "While was launching stream publisher daemon already found a active stream for {:?}",
                streaming_key
            );
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            let client = StreamPublisherClient {
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
            let r =
                _stream_publisher_daemon(streaming_key, network, command_receiver, record).await;
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
