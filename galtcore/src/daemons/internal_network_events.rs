// SPDX-License-Identifier: AGPL-3.0-only

use std::time::{Duration, SystemTime};

use anyhow::Context;
use libp2p::gossipsub::GossipsubMessage;
use libp2p::request_response::ResponseChannel;
use libp2p::PeerId;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};

use super::cm::rtmp::handlers::{RespondPaymentInfo, RespondRTMPStreamingInfo};
use super::cm::simple_file::RespondSimpleFileInfo;
use super::cm::{ClientCommand, SentRTMPResponseStats};
use super::gossip_listener::GossipListenerClient;
use crate::protocols::kademlia_record::KademliaRecord;
use crate::protocols::payment_info::{PaymentInfoRequest, PaymentInfoResponseResult};
use crate::protocols::rtmp_streaming::{RTMPStreamingRequest, WrappedRTMPStreamingResponseResult};
use crate::protocols::simple_file_exchange::{SimpleFileRequest, SimpleFileResponse};


#[derive(Debug)]
pub enum InternalNetworkEvent {
    InboundFileRequest {
        peer: PeerId,
        request: SimpleFileRequest,
        filename: String,
        channel: ResponseChannel<Result<SimpleFileResponse, String>>,
    },
    InboundRTMPStreamingDataRequest {
        peer: PeerId,
        request: RTMPStreamingRequest,
        channel: ResponseChannel<WrappedRTMPStreamingResponseResult>,
    },
    InboundPaymentInfoRequest {
        peer: PeerId,
        request: PaymentInfoRequest,
        channel: ResponseChannel<PaymentInfoResponseResult>,
    },
    ReceivedGossip {
        message: GossipsubMessage,
    },
    PutRecord {
        record: KademliaRecord,
    },
    SentRTMPResponseStats {
        now: SystemTime,
        peer: PeerId,
        write_duration: Duration,
        written_bytes: usize,
        responses_count: u32,
    },
}

pub async fn run_loop(
    mut event_receiver: UnboundedReceiver<InternalNetworkEvent>,
    client_command_sender: Sender<ClientCommand>,
    gossip_listener_client: GossipListenerClient,
) -> anyhow::Result<()> {
    loop {
        // TODO: consider dropping events if we can't handle everything
        tokio::task::yield_now().await;
        match event_receiver.recv().await.context("loop finished")? {
            InternalNetworkEvent::InboundFileRequest {
                peer,
                request,
                filename,
                channel,
            } => {
                if client_command_sender
                    .send(ClientCommand::RespondSimpleFile(RespondSimpleFileInfo {
                        peer,
                        request,
                        filename,
                        channel,
                    }))
                    .await
                    .is_err()
                {
                    anyhow::bail!("receiver died")
                }
            }
            InternalNetworkEvent::InboundRTMPStreamingDataRequest {
                peer,
                request,
                channel,
            } => {
                if client_command_sender
                    .send(ClientCommand::RespondRTMPStreamingRequest(
                        RespondRTMPStreamingInfo {
                            peer,
                            request,
                            channel,
                        },
                    ))
                    .await
                    .is_err()
                {
                    anyhow::bail!("receiver died")
                }
            }
            InternalNetworkEvent::InboundPaymentInfoRequest {
                peer,
                request,
                channel,
            } => {
                if client_command_sender
                    .send(ClientCommand::RespondPaymentInfoRequest(
                        RespondPaymentInfo {
                            peer,
                            request,
                            channel,
                        },
                    ))
                    .await
                    .is_err()
                {
                    anyhow::bail!("receiver died")
                }
            }
            InternalNetworkEvent::SentRTMPResponseStats {
                peer,
                now,
                written_bytes,
                write_duration,
                responses_count,
            } => {
                if client_command_sender
                    .send(ClientCommand::FeedSentRTMPResponseStats(
                        SentRTMPResponseStats {
                            peer,
                            now,
                            written_bytes,
                            write_duration,
                            responses_count,
                        },
                    ))
                    .await
                    .is_err()
                {
                    anyhow::bail!("receiver died")
                }
            }
            InternalNetworkEvent::ReceivedGossip { message } => {
                gossip_listener_client.whisper(message).await
            }
            InternalNetworkEvent::PutRecord { record } => {
                if let KademliaRecord::Rtmp(r) = record {
                    gossip_listener_client.notify_rtmp_record(r).await;
                }
            }
        }
    }
}
