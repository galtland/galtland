// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

use anyhow::Context;
use libp2p::floodsub::FloodsubMessage;
// use libp2p::gossipsub::GossipsubMessage;
use libp2p::request_response::ResponseChannel;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};

use super::cm::simple_file::RespondSimpleFileInfo;
use super::cm::streaming::handlers::{RespondPaymentInfo, RespondStreamingInfo};
use super::cm::{ClientCommand, SentStreamingResponseStats};
use super::gossip_listener::GossipListenerClient;
use crate::protocols::kademlia_record::KademliaRecord;
use crate::protocols::media_streaming::{StreamingRequest, WrappedStreamingResponseResult};
use crate::protocols::payment_info::{PaymentInfoRequest, PaymentInfoResponseResult};
use crate::protocols::simple_file_exchange::{SimpleFileRequest, SimpleFileResponse};


#[derive(Debug)]
pub enum InternalNetworkEvent {
    InboundFileRequest {
        peer: PeerId,
        request: SimpleFileRequest,
        filename: String,
        channel: ResponseChannel<Result<SimpleFileResponse, String>>,
    },
    InboundStreamingDataRequest {
        peer: PeerId,
        request: StreamingRequest,
        channel: ResponseChannel<WrappedStreamingResponseResult>,
    },
    InboundPaymentInfoRequest {
        peer: PeerId,
        request: PaymentInfoRequest,
        channel: ResponseChannel<PaymentInfoResponseResult>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum BroadcastableNetworkEvent {
    ReceivedGossip {
        // message: GossipsubMessage,
        message: FloodsubMessage,
    },
    PutRecord {
        record: KademliaRecord,
    },
    SentStreamingResponseStats {
        now: instant::Instant,
        peer: PeerId,
        write_duration: Duration,
        written_bytes: usize,
        responses_count: u32,
    },
    ConnectionEstablished {
        peer: PeerId,
        endpoint: Multiaddr,
    },
}

pub async fn run_loop(
    mut event_receiver: UnboundedReceiver<InternalNetworkEvent>,
    mut broadcast_receiver: broadcast::Receiver<BroadcastableNetworkEvent>,
    client_command_sender: Sender<ClientCommand>,
    gossip_listener_client: GossipListenerClient,
) -> anyhow::Result<()> {
    loop {
        // TODO: consider dropping events if we can't handle everything
        tokio::task::yield_now().await;
        tokio::select! {
            event = event_receiver.recv() => {
                handle_internal_network_event(&client_command_sender, event.context("loop finished")?).await?
            },
            event = broadcast_receiver.recv() => {
                handle_broadcast_network_event(&client_command_sender, &gossip_listener_client, event.context("loop finished")?).await?
            }
        };
    }
}

async fn handle_broadcast_network_event(
    client_command_sender: &Sender<ClientCommand>,
    gossip_listener_client: &GossipListenerClient,
    event: BroadcastableNetworkEvent,
) -> anyhow::Result<()> {
    match event {
        BroadcastableNetworkEvent::SentStreamingResponseStats {
            peer,
            now,
            written_bytes,
            write_duration,
            responses_count,
        } => {
            if client_command_sender
                .send(ClientCommand::FeedSentStreamingResponseStats(
                    SentStreamingResponseStats {
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
        BroadcastableNetworkEvent::ReceivedGossip { message } => {
            gossip_listener_client.whisper(message).await
        }
        BroadcastableNetworkEvent::PutRecord { record } => {
            if let KademliaRecord::MediaStreaming(r) = record {
                gossip_listener_client.notify_streaming_record(r).await;
            }
        }
        BroadcastableNetworkEvent::ConnectionEstablished { .. } => {}
    };
    Ok(())
}

async fn handle_internal_network_event(
    client_command_sender: &Sender<ClientCommand>,
    event: InternalNetworkEvent,
) -> anyhow::Result<()> {
    match event {
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
        InternalNetworkEvent::InboundStreamingDataRequest {
            peer,
            request,
            channel,
        } => {
            if client_command_sender
                .send(ClientCommand::RespondStreamingRequest(
                    RespondStreamingInfo {
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
    };
    Ok(())
}
