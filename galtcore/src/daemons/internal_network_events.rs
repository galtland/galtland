// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

use anyhow::Context;
use libp2p::floodsub::FloodsubMessage;
use libp2p::identify::IdentifyInfo;
// use libp2p::gossipsub::GossipsubMessage;
use libp2p::request_response::ResponseChannel;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;

use super::gossip_listener::GossipListenerClient;
use crate::cm::modules::payment_info::RespondPaymentInfo;
use crate::cm::modules::simple_file::RespondSimpleFileInfo;
use crate::cm::modules::streaming::RespondStreamingInfo;
use crate::cm::{self, SentStreamingResponseStats, SharedGlobalState};
use crate::configuration::Configuration;
use crate::networkbackendclient::NetworkBackendClient;
use crate::protocols::galt_identify::{GaltIdentifyRequest, GaltIdentifyResponseResult};
use crate::protocols::kademlia_record::KademliaRecord;
use crate::protocols::media_streaming::{StreamingRequest, WrappedStreamingResponseResult};
use crate::protocols::payment_info::{PaymentInfoRequest, PaymentInfoResponseResult};
use crate::protocols::simple_file_exchange::{SimpleFileRequest, SimpleFileResponse};
use crate::protocols::NodeIdentity;
use crate::utils;


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
    GaltIdentifyRequest {
        peer: PeerId,
        request: GaltIdentifyRequest,
        channel: ResponseChannel<GaltIdentifyResponseResult>,
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
    PingInfo {
        peer: PeerId,
        rtt: Option<Duration>,
    },
    IdentifyInfo {
        peer: PeerId,
        info: IdentifyInfo,
    },
}

pub async fn run_loop(
    mut event_receiver: UnboundedReceiver<InternalNetworkEvent>,
    mut broadcast_receiver: broadcast::Receiver<BroadcastableNetworkEvent>,
    gossip_listener_client: GossipListenerClient,
    opt: Configuration,
    identity: NodeIdentity,
    shared_global_state: SharedGlobalState,
    network: NetworkBackendClient,
) -> anyhow::Result<()> {
    loop {
        // TODO: consider dropping events if we can't handle everything
        tokio::task::yield_now().await;
        tokio::select! {
            event = event_receiver.recv() => {
                utils::spawn_and_log_error(handle_internal_network_event(opt.clone(), identity.clone(), shared_global_state.clone(), network.clone(), event.context("loop finished")?))
            },
            event = broadcast_receiver.recv() => {
                utils::spawn_and_log_error(handle_broadcast_network_event(gossip_listener_client.clone(), identity.clone(), shared_global_state.clone(), network.clone(), event.context("loop finished")?))
            }
        };
    }
}

async fn handle_broadcast_network_event(
    gossip_listener_client: GossipListenerClient,
    identity: NodeIdentity,
    shared_global_state: SharedGlobalState,
    network: NetworkBackendClient,
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
            let info = SentStreamingResponseStats {
                peer,
                now,
                written_bytes,
                write_duration,
                responses_count,
            };
            cm::feed_streaming_response_stats(shared_global_state, info).await;
        }
        BroadcastableNetworkEvent::ReceivedGossip { message } => {
            gossip_listener_client.whisper(message).await
        }
        BroadcastableNetworkEvent::PutRecord { record } => {
            if let KademliaRecord::MediaStreaming(r) = record {
                gossip_listener_client.notify_streaming_record(r).await;
            }
        }
        BroadcastableNetworkEvent::ConnectionEstablished { peer, .. } => {
            cm::modules::galt_identify::handle_establish_with_peer(
                &identity,
                shared_global_state,
                network,
                peer,
            )
            .await?;
        }
        BroadcastableNetworkEvent::PingInfo { peer, rtt } => {
            shared_global_state
                .peer_control
                .lock()
                .await
                .add_ping_info(peer, rtt);
        }
        BroadcastableNetworkEvent::IdentifyInfo { peer, info } => {
            shared_global_state
                .peer_control
                .lock()
                .await
                .add_identify_info(peer, info);
        }
    };
    Ok(())
}

async fn handle_internal_network_event(
    opt: Configuration,
    identity: NodeIdentity,
    shared_state_global: SharedGlobalState,
    network: NetworkBackendClient,
    event: InternalNetworkEvent,
) -> anyhow::Result<()> {
    match event {
        InternalNetworkEvent::InboundFileRequest {
            peer,
            request,
            filename,
            channel,
        } => {
            let info = RespondSimpleFileInfo {
                peer,
                request,
                filename,
                channel,
            };
            cm::modules::simple_file::handle_respond(shared_state_global, network, info).await?;
        }
        InternalNetworkEvent::InboundStreamingDataRequest {
            peer,
            request,
            channel,
        } => {
            let info = RespondStreamingInfo {
                peer,
                request,
                channel,
            };
            cm::modules::streaming::respond(opt, identity, shared_state_global, network, info)
                .await?;
        }
        InternalNetworkEvent::InboundPaymentInfoRequest {
            peer,
            request,
            channel,
        } => {
            let info = RespondPaymentInfo {
                peer,
                request,
                channel,
            };
            cm::modules::payment_info::handle_respond(network, info).await?
        }
        InternalNetworkEvent::GaltIdentifyRequest {
            peer,
            request,
            channel,
        } => {
            let request = crate::protocols::galt_identify::RequestEvent {
                peer,
                request,
                channel,
            };
            cm::modules::galt_identify::handle_respond(
                &identity,
                shared_state_global,
                network,
                request,
            )
            .await?
        }
    };
    Ok(())
}
