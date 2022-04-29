// SPDX-License-Identifier: AGPL-3.0-only

pub mod dcutr_behaviour;
pub mod delegated_streaming;
pub mod gossip;
pub mod kademlia_record;
pub mod media_streaming;
pub mod payment_info;
pub mod relay_server;
pub mod simple_file_exchange;
pub mod webrtc_signaling;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Context;
use kademlia_record::KademliaRecord;
use libp2p::core::ConnectedPoint;
use libp2p::identify::{Identify, IdentifyEvent};
use libp2p::identity::Keypair;
use libp2p::kad::record::Key;
use libp2p::kad::store::{MemoryStore, RecordStore};
use libp2p::kad::{
    AddProviderOk, GetProvidersError, Kademlia, KademliaEvent, PutRecordError, PutRecordOk,
    QueryResult,
};
// use libp2p::mdns::MdnsEvent;
use libp2p::relay::v2::relay;
use libp2p::rendezvous::{self, Cookie};
use libp2p::request_response::{RequestResponse, RequestResponseEvent};
use libp2p::swarm::{DialError, SwarmEvent};
use libp2p::{dcutr, floodsub, multiaddr, ping, PeerId, Swarm};
use log::{debug, info, warn};
use media_streaming::{StreamingRequest, WrappedStreamingResponseResult};
use simple_file_exchange::{SimpleFileRequest, SimpleFileResponse};
use tokio::sync::{broadcast, mpsc, oneshot};

use self::delegated_streaming::{DelegatedStreamingRequest, DelegatedStreamingResponseResult};
use self::media_streaming::StreamingResponseResult;
use self::payment_info::{PaymentInfoRequest, PaymentInfoResponseResult};
use self::webrtc_signaling::{SignalingRequestOrResponse, WebRtcSignalingResponseResult};
use crate::configuration::Configuration;
use crate::daemons::internal_network_events::{BroadcastableNetworkEvent, InternalNetworkEvent};
use crate::networkbackendclient::{NetworkBackendCommand, RequestStreamingDataParams};
use crate::utils;

pub const RENDEZVOUS_NAMESPACE: &str = "rendezvous";

// pub(crate) fn handle_mdns_event(event: MdnsEvent, _swarm: &mut Swarm<ComposedBehaviour>) {
//     match event {
//         MdnsEvent::Discovered(_list) => {
//             let mut added_new_peers = false;
//             for (peer_id, multiaddr) in list {
//                 info!("MdnsEvent::Discovered {} at {}", peer_id, multiaddr);
//                 added_new_peers = true;
//                 swarm
//                     .behaviour_mut()
//                     .kademlia
//                     .add_address(&peer_id, multiaddr);
//             }
//             if added_new_peers {
//                 swarm
//                     .behaviour_mut()
//                     .kademlia
//                     .bootstrap()
//                     .expect("be able to start bootstrap process");
//             }
//         }
//         MdnsEvent::Expired(expired) => {
//             for (peer, addr) in expired {
//                 info!("Expired {} at {}", peer, addr);
//             }
//         }
//     }
// }

pub(crate) fn handle_kademlia_event(message: KademliaEvent, swarm: &mut Swarm<ComposedBehaviour>) {
    match message {
        KademliaEvent::OutboundQueryCompleted { id, result, .. } => match result {
            QueryResult::GetProviders(Ok(ok)) => {
                if ok.providers.is_empty() {
                    warn!("No providers for {:?}", ok.key);
                }
                for peer in &ok.providers {
                    debug!("Peer {} provides key {:?}", peer, ok.key);
                }
                swarm
                    .behaviour_mut()
                    .state
                    .pending_get_providers
                    .remove(&id)
                    .expect("Completed query to be previously pending")
                    .send(Ok(ok.providers))
                    .expect("Receiver not to be dropped");
            }
            QueryResult::GetProviders(Err(error)) => {
                warn!("Timeout getting providers on query {:?}", id);
                swarm
                    .behaviour_mut()
                    .state
                    .pending_get_providers
                    .remove(&id)
                    .expect("Completed query to be previously pending")
                    .send(Err(error))
                    .expect("Receiver not to be dropped");
            }
            QueryResult::GetRecord(Ok(ok)) if ok.records.is_empty() => {
                log::warn!("Received empty ok records: {:?}", ok);
            }
            QueryResult::GetRecord(Ok(ok)) => {
                log::trace!("QueryResult::GetRecord got {:?}", ok);
                let b = swarm.behaviour_mut();
                let len = ok.records.len();
                let key = ok.records[0].record.key.clone();
                let cached_record = b.state.cached_kademlia_records.remove(&key);
                let record = cached_record.or_else(|| {
                    ok.records.into_iter().find_map(|r| {
                        let kr = (&r.record).try_into();
                        if let Err(e) = &kr {
                            log::warn!("Error converting record: {}", e);
                        }
                        kr.ok()
                    })
                });
                if let Some(record) = record {
                    log::debug!("QueryResult::GetRecord found valid {:?}", record);
                    let senders = b.state.pending_get_records.remove(&key).unwrap_or_default();
                    for sender in senders {
                        if sender.send(Ok(record.clone())).is_err() {
                            log::warn!("Failed to send response");
                        };
                    }
                    b.state.cached_kademlia_records.insert(key, record);
                } else {
                    log::warn!("Received {} records and found zero valid", len);
                    let senders = b.state.pending_get_records.remove(&key).unwrap_or_default();
                    for sender in senders {
                        if sender
                            .send(Err(anyhow::anyhow!(
                                "Received {} records and found zero valid",
                                len
                            )))
                            .is_err()
                        {
                            log::warn!("Failed to send response");
                        };
                    }
                }
            }
            QueryResult::GetRecord(Err(err)) => {
                warn!("Failed to get record: {:?}", err);
                let key = err.key();
                let senders = swarm
                    .behaviour_mut()
                    .state
                    .pending_get_records
                    .remove(key)
                    .unwrap_or_default();
                for sender in senders {
                    if sender
                        .send(Err(anyhow::anyhow!("Failed to get record: {:?}", err)))
                        .is_err()
                    {
                        log::warn!("Failed to send response");
                    };
                }
            }
            QueryResult::PutRecord(Ok(PutRecordOk { key })) => {
                debug!("Successfully put record {:?}", key);
                let (record, sender) = swarm
                    .behaviour_mut()
                    .state
                    .pending_put_records
                    .remove(&id)
                    .expect("Completed action to be previously pending");
                sender.send(Ok(())).expect("Receiver not to be dropped");
                if let Err(e) = swarm
                    .behaviour_mut()
                    .broadcast_event_sender
                    .send(BroadcastableNetworkEvent::PutRecord { record })
                {
                    log::warn!("Error sending BroadcastableNetworkEvent::PutRecord: {}", e);
                }
            }
            QueryResult::PutRecord(Err(PutRecordError::QuorumFailed { key, .. })) => {
                warn!("Failed to put record {:?} because lack of quorum", key);
                let (_, sender) = swarm
                    .behaviour_mut()
                    .state
                    .pending_put_records
                    .remove(&id)
                    .expect("Completed action to be previously pending");
                sender
                    .send(Err(anyhow::anyhow!("PutRecordError::QuorumFailed")))
                    .expect("Receiver not to be dropped");
            }
            QueryResult::PutRecord(Err(PutRecordError::Timeout { key, .. })) => {
                warn!("Failed to put record {:?} because of timeout", key);
                let (_, sender) = swarm
                    .behaviour_mut()
                    .state
                    .pending_put_records
                    .remove(&id)
                    .expect("Completed action to be previously pending");
                sender
                    .send(Err(anyhow::anyhow!("PutRecordError::Timeout")))
                    .expect("Receiver not to be dropped");
            }
            QueryResult::StartProviding(Ok(AddProviderOk { key })) => {
                info!("Successfully started providing key {:?}", key);
                let b = swarm.behaviour_mut();
                let result = b
                    .state
                    .pending_start_providing
                    .remove(&id)
                    .expect("Completed action to be previously pending");
                let sender = match result {
                    StartProvidingAction::SimpleFile { filename, sender } => {
                        b.state.published_files_mapping.insert(key, filename); // FIXME: check if already exists and maybe return error, also remove if publish fails
                        sender
                    }
                    StartProvidingAction::MediaStreaming { sender } => sender,
                };
                sender.send(Ok(())).expect("Receiver not to be dropped");
            }
            QueryResult::StartProviding(Err(err)) => {
                warn!("Failed to start providing key: {:?}", err);
                let result = swarm
                    .behaviour_mut()
                    .state
                    .pending_start_providing
                    .remove(&id)
                    .expect("Completed action to be previously pending");
                let sender = match result {
                    StartProvidingAction::SimpleFile { sender, .. } => sender,
                    StartProvidingAction::MediaStreaming { sender, .. } => sender,
                };
                sender.send(Err(err)).expect("Receiver not to be dropped");
            }
            QueryResult::Bootstrap(i) => {
                log::debug!("QueryResult::Bootstrap: {:?}", i);
            }
            QueryResult::GetClosestPeers(i) => {
                log::debug!("QueryResult::GetClosestPeers: {:?}", i);
            }
            QueryResult::RepublishProvider(i) => {
                log::debug!("QueryResult::RepublishProvider: {:?}", i);
            }
            QueryResult::RepublishRecord(i) => {
                log::debug!("QueryResult::RepublishRecord: {:?}", i);
            }
        },
        KademliaEvent::InboundRequest { request } => match request {
            libp2p::kad::InboundRequest::FindNode { num_closer_peers } => {
                log::debug!("InboundRequest::FindNode {}", num_closer_peers,);
            }
            libp2p::kad::InboundRequest::GetProvider {
                num_closer_peers,
                num_provider_peers,
            } => {
                log::debug!(
                    "InboundRequest::GetProvider {} {}",
                    num_closer_peers,
                    num_provider_peers
                );
            }
            libp2p::kad::InboundRequest::AddProvider {
                record: Some(provider_record),
            } => {
                log::debug!("InboundRequest::GetProvider {:?}", provider_record);
                swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .add_provider(provider_record)
                    .expect("to succeed");
            }
            libp2p::kad::InboundRequest::AddProvider { .. } => {
                log::error!("InboundRequest::GetProvider: configuration should be KademliaStoreInserts::FilterBoth which fills record")
            }
            libp2p::kad::InboundRequest::GetRecord {
                num_closer_peers,
                present_locally,
            } => {
                log::debug!(
                    "InboundRequest::GetRecord {} {}",
                    num_closer_peers,
                    present_locally
                );
            }
            libp2p::kad::InboundRequest::PutRecord {
                source,
                connection: _,
                record: Some(record),
            } => {
                log::debug!("InboundRequest::PutRecord {:?} from {}", record, source);
                let validation: anyhow::Result<KademliaRecord> = (&record).try_into();
                match validation {
                    Ok(_) => swarm
                        .behaviour_mut()
                        .kademlia
                        .store_mut()
                        .put(record)
                        .expect("to succeed"), // FIXME: the data store should be way bigger and handle out of memory gracefully
                    Err(e) => log::warn!("Ignoring invalid record {:?}: {}", record, e),
                };
            }
            libp2p::kad::InboundRequest::PutRecord {
                source: _,
                connection: _,
                record: _,
            } => {
                log::error!("InboundRequest::PutRecord: configuration should be KademliaStoreInserts::FilterBoth which fills record")
            }
        },
        i @ KademliaEvent::RoutingUpdated { .. } => {
            log::debug!("KademliaEvent::RoutingUpdated: {:?}", i);
        }
        i @ KademliaEvent::UnroutablePeer { .. } => {
            log::debug!("KademliaEvent::UnroutablePeer: {:?}", i);
        }
        i @ KademliaEvent::RoutablePeer { .. } => {
            log::debug!("KademliaEvent::RoutablePeer: {:?}", i);
        }
        i @ KademliaEvent::PendingRoutablePeer { .. } => {
            log::debug!("KademliaEvent::PendingRoutablePeer: {:?}", i);
        }
    }
}

pub(crate) fn handle_rendezvous_server(event: rendezvous::server::Event) {
    match event {
        rendezvous::server::Event::DiscoverServed {
            enquirer,
            registrations,
        } => {
            if !registrations.is_empty() {
                info!(
                    "Served peer {} with {} registrations",
                    enquirer,
                    registrations.len()
                )
            } else {
                debug!("No registrations available for peer {}", enquirer,)
            }
        }
        rendezvous::server::Event::DiscoverNotServed { enquirer, error } => info!(
            "rendezvous::server::Event::DiscoverNotServed {}: {:?}",
            enquirer, error
        ),
        rendezvous::server::Event::PeerRegistered {
            peer,
            registration: _,
        } => {
            info!("rendezvous::server::Event::PeerRegistered {}", peer)
        }
        rendezvous::server::Event::PeerNotRegistered {
            peer,
            namespace: _,
            error,
        } => info!(
            "rendezvous::server::Event::PeerNotRegistered {}: {:?}",
            peer, error
        ),
        rendezvous::server::Event::PeerUnregistered { peer, namespace: _ } => {
            info!("rendezvous::server::Event::PeerUnregistered {}", peer)
        }
        rendezvous::server::Event::RegistrationExpired(_) => {
            info!("rendezvous::server::Event::RegistrationExpired")
        }
    }
}

pub(crate) fn handle_rendezvous_client(
    event: rendezvous::client::Event,
    swarm: &mut Swarm<ComposedBehaviour>,
) {
    match event {
        rendezvous::client::Event::Discovered {
            rendezvous_node,
            registrations,
            cookie: new_cookie,
        } => {
            if registrations.is_empty() {
                debug!("No registrations available")
            }
            if let Some(state) = swarm
                .behaviour_mut()
                .state
                .rendezvous_peers
                .get_mut(&rendezvous_node)
            {
                state.cookie.replace(new_cookie);
            };

            let my_peer_id = *swarm.local_peer_id();

            let mut added_new_peers = false;
            for registration in registrations {
                for address in registration.record.addresses() {
                    let peer = registration.record.peer_id();
                    if peer == my_peer_id {
                        log::trace!("rendezvous::client::Event::Discovered ignoring ourselves");
                        continue;
                    }
                    log::info!(
                        "rendezvous::client::Event::Discovered peer {} at {}",
                        peer,
                        address
                    );

                    let address_with_p2p = address
                        .clone()
                        .with(multiaddr::Protocol::P2p(*peer.as_ref()));

                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer, address_with_p2p);

                    added_new_peers = true;
                }
            }
            if added_new_peers {
                log::debug!("Starting a kademlia bootstrap because new peers have been added");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .bootstrap()
                    .expect("to be able to start bootstrap process");
            }
        }
        rendezvous::client::Event::DiscoverFailed {
            rendezvous_node: _,
            namespace: _,
            error,
        } => warn!("rendezvous::client::Event::DiscoverFailed {:?}", error),
        rendezvous::client::Event::Registered {
            rendezvous_node,
            ttl,
            namespace,
        } => log::debug!(
            "Registered for namespace '{}' at rendezvous point {} for the next {} seconds",
            namespace,
            rendezvous_node,
            ttl
        ),
        rendezvous::client::Event::RegisterFailed(e) => {
            warn!("rendezvous::client::Event::RegisterFailed: {}", e)
        }
        rendezvous::client::Event::Expired { peer } => {
            info!("rendezvous::client::Event::Expired {}", peer)
        }
    }
}

pub(crate) fn handle_identity(
    event: IdentifyEvent,
    opt: &Configuration,
    swarm: &mut Swarm<ComposedBehaviour>,
) {
    match event {
        IdentifyEvent::Received { peer_id, info } => {
            debug!(
                "IdentifyEvent::Received from {} our address is {}",
                peer_id, info.observed_addr
            );
            if !opt.disable_rendezvous_register
                && swarm
                    .behaviour()
                    .state
                    .rendezvous_peers
                    .contains_key(&peer_id)
            {
                swarm.behaviour_mut().rendezvous_client.register(
                    rendezvous::Namespace::from_static(RENDEZVOUS_NAMESPACE),
                    peer_id,
                    Some(rendezvous::MIN_TTL),
                );
            }
        }
        IdentifyEvent::Sent { peer_id } => debug!("IdentifyEvent::Sent {}", peer_id),
        IdentifyEvent::Pushed { peer_id } => info!("IdentifyEvent::Pushed {}", peer_id),
        IdentifyEvent::Error { peer_id, error: _ } => info!("IdentifyEvent::Error {}", peer_id),
    }
}

pub(crate) fn handle_ping(event: ping::Event, swarm: &mut Swarm<ComposedBehaviour>) {
    match event.result {
        Ok(ping::Success::Ping { rtt }) => {
            log::debug!("Success ping {:?} to {}", rtt, event.peer);
            let peer_statistics = swarm
                .behaviour_mut()
                .state
                .peer_statistics
                .entry(event.peer)
                .or_default();
            peer_statistics.latency.replace(rtt);
        }
        Ok(ping::Success::Pong) => log::debug!("Success pong from {}", event.peer),
        Err(e) => {
            log::info!("Ping failure {} to {}", e, event.peer);
            let peer_statistics = swarm
                .behaviour_mut()
                .state
                .peer_statistics
                .entry(event.peer)
                .or_default();
            peer_statistics.latency.take();
        }
    }
}

pub fn handle_network_backend_command(
    command: NetworkBackendCommand,
    swarm: &mut Swarm<ComposedBehaviour>,
) -> anyhow::Result<()> {
    match command {
        NetworkBackendCommand::StartProvidingSimpleFile {
            key,
            filename,
            sender,
        } => {
            let record_key = libp2p::kad::record::Key::new(&key);
            let b = swarm.behaviour_mut();
            let query_id = b
                .kademlia
                .start_providing(record_key)
                .context("Expected no store error")?;
            b.state.pending_start_providing.insert(
                query_id,
                StartProvidingAction::SimpleFile { filename, sender },
            );
        }
        NetworkBackendCommand::StartProvidingStreaming {
            kad_key,
            streaming_key: _,
            sender,
        } => {
            let record_key = libp2p::kad::record::Key::new(&kad_key);
            let b = swarm.behaviour_mut();
            let query_id = b
                .kademlia
                .start_providing(record_key)
                .context("Expected no store error")?; //FIXME: we need some sort of store cleanup
            b.state
                .pending_start_providing
                .insert(query_id, StartProvidingAction::MediaStreaming { sender });
        }
        NetworkBackendCommand::GetProviders { key, sender } => {
            let key = libp2p::kad::record::Key::new(&key);
            let b = swarm.behaviour_mut();
            let query_id = b.kademlia.get_providers(key);
            b.state.pending_get_providers.insert(query_id, sender);
        }
        NetworkBackendCommand::GetRecord { key, sender } => {
            let b = swarm.behaviour_mut();
            if let Some(cached_record) = b.state.cached_kademlia_records.get(&key) {
                log::debug!("NetworkBackendCommand::GetRecord got cached for {:?}", key);
                if sender.send(Ok(cached_record.clone())).is_err() {
                    log::warn!("Failed to send response");
                };
            } else {
                log::debug!(
                    "NetworkBackendCommand::GetRecord will request for {:?}",
                    key
                );
                let kad_key = libp2p::kad::record::Key::new(&key);
                b.kademlia.get_record(kad_key, libp2p::kad::Quorum::One);
                match b.state.pending_get_records.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().push(sender);
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(vec![sender]);
                    }
                };
            }
        }
        NetworkBackendCommand::PutRecord { record, sender } => match record.clone().into_record() {
            Ok(r) => {
                let b = swarm.behaviour_mut();
                let query_id = b
                    .kademlia
                    .put_record(r, libp2p::kad::Quorum::One)
                    .expect("No store error");
                b.state
                    .pending_put_records
                    .insert(query_id, (record, sender));
            }
            Err(e) => {
                if let Err(e) = sender.send(Err(e)) {
                    log::warn!("Error sending error to receiver: {:?}", e);
                }
            }
        },
        NetworkBackendCommand::RequestSimpleFile {
            file_request,
            peer,
            sender,
        } => {
            let b = swarm.behaviour_mut();
            let request_id = b.simple_file_exchange.send_request(&peer, file_request);
            b.state
                .pending_simple_file_request
                .insert(request_id, sender);
        }
        NetworkBackendCommand::RespondSimpleFile { response, channel } => {
            if swarm
                .behaviour_mut()
                .simple_file_exchange
                .send_response(channel, response)
                .is_err()
            {
                log::warn!("Expected connection to peer to be still open");
            }
        }
        NetworkBackendCommand::GetPublishedFileName { key, sender } => {
            let key = libp2p::kad::record::Key::new(&key);
            let filename = swarm
                .behaviour_mut()
                .state
                .published_files_mapping
                .get(&key)
                .cloned();
            sender.send(filename).map_err(utils::send_error)?;
        }
        NetworkBackendCommand::RequestStreamingData {
            params: RequestStreamingDataParams { peer, request },
            sender,
        } => {
            let b = swarm.behaviour_mut();
            let request_id = b.media_streaming.send_request(&peer, request);
            b.state.pending_streaming_request.insert(request_id, sender);
        }
        NetworkBackendCommand::RespondStreamingData {
            peer,
            response,
            channel,
        } => {
            if swarm
                .behaviour_mut()
                .media_streaming
                .send_response(
                    channel,
                    WrappedStreamingResponseResult {
                        peer: Some(peer),
                        response,
                    },
                )
                .is_err()
            {
                log::warn!("Expected connection to peer to be still open while sending response");
            };
        }
        NetworkBackendCommand::GetPeerStatistics { sender } => {
            let peer_statistics = swarm.behaviour().state.peer_statistics.clone();
            sender.send(peer_statistics).map_err(utils::send_error)?;
        }
        NetworkBackendCommand::RespondPaymentInfo { response, channel } => {
            if swarm
                .behaviour_mut()
                .payment_info
                .send_response(channel, response)
                .is_err()
            {
                log::warn!("Expected connection to peer to be still open while sending response");
            };
        }
        NetworkBackendCommand::GetSwarmNetworkInfo { sender } => {
            sender
                .send(swarm.network_info())
                .map_err(utils::send_error)?;
        }
        NetworkBackendCommand::PublishGossip {
            data,
            topic,
            sender,
        } => {
            //     match swarm.behaviour_mut().gossip.publish(topic, data) {
            //     Ok(_) => sender.send(Ok(())).map_err(utils::send_error)?,
            //     Err(e) => sender.send(Err(e)).map_err(utils::send_error)?,
            // }
            swarm.behaviour_mut().gossip.publish(topic, data);
            sender.send(()).map_err(utils::send_error)?;
        }
        NetworkBackendCommand::StopProviding { kad_key, sender } => {
            swarm.behaviour_mut().kademlia.stop_providing(&kad_key);
            sender.send(()).map_err(utils::send_error)?;
        }
        NetworkBackendCommand::RemoveRecord { kad_key, sender } => {
            swarm.behaviour_mut().kademlia.remove_record(&kad_key);
            sender.send(()).map_err(utils::send_error)?;
        }
        NetworkBackendCommand::RespondWebRtcSignaling { response, channel } => {
            if swarm
                .behaviour_mut()
                .webrtc_signaling
                .send_response(channel, response)
                .is_err()
            {
                log::warn!("Expected connection to peer to be still open while sending response");
            };
        }
        NetworkBackendCommand::RequestWebRtcSignaling {
            request,
            peer,
            sender,
        } => {
            let b = swarm.behaviour_mut();
            let request_id = b.webrtc_signaling.send_request(&peer, request);
            b.state
                .pending_webrtc_signaling_request
                .insert(request_id, sender);
        }
        NetworkBackendCommand::RequestDelegatedStreaming {
            request,
            peer,
            sender,
        } => {
            let b = swarm.behaviour_mut();
            let request_id = b.delegated_streaming.send_request(&peer, request);
            b.state
                .pending_delegated_streaming_request
                .insert(request_id, sender);
        }
        NetworkBackendCommand::RespondDelegatedStreaming { response, channel } => {
            if swarm
                .behaviour_mut()
                .delegated_streaming
                .send_response(channel, response)
                .is_err()
            {
                log::warn!("Expected connection to peer to be still open while sending response");
            };
        }
    };
    Ok(())
}

pub fn handle_swarm_event<E: std::fmt::Debug>(
    event: SwarmEvent<ComposedEvent, E>,
    opt: &Configuration,
    swarm: &mut Swarm<ComposedBehaviour>,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!("SwarmEvent::NewListenAddr {}", address);
        }
        SwarmEvent::IncomingConnection {
            local_addr,
            send_back_addr,
        } => {
            info!(
                "SwarmEvent::IncomingConnection {} {}",
                local_addr, send_back_addr
            );
        }
        SwarmEvent::IncomingConnectionError {
            local_addr,
            send_back_addr,
            error,
        } => {
            warn!(
                "SwarmEvent::IncomingConnectionError {} {}: {}",
                local_addr, send_back_addr, error
            );
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            endpoint,
            num_established,
            concurrent_dial_errors: _,
        } => {
            info!(
                "SwarmEvent::ConnectionEstablished {} at {:?} (num_established: {num_established:?})",
                peer_id, endpoint
            );
            match &endpoint {
                ConnectedPoint::Dialer {
                    address,
                    role_override: _,
                } => {
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, address.clone());
                }
                ConnectedPoint::Listener { send_back_addr, .. } => {
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, send_back_addr.clone());
                }
            };
            // log::debug!("    Starting a kademlia bootstrap because connected to a new peer");
            // swarm
            //     .behaviour_mut()
            //     .kademlia
            //     .bootstrap()
            //     .expect("to be able to start bootstrap process");

            if !opt.disable_rendezvous_discover {
                swarm.behaviour_mut().rendezvous_client.discover(
                    Some(rendezvous::Namespace::from_static(RENDEZVOUS_NAMESPACE)),
                    None,
                    None,
                    peer_id,
                );
                let endpoint = endpoint.get_remote_address();
                if opt.rendezvous_addresses.contains(endpoint) {
                    log::info!(
                        "    Connected to rendezvous point {:?} ({})",
                        endpoint,
                        peer_id
                    );
                    swarm
                        .behaviour_mut()
                        .state
                        .rendezvous_peers
                        .insert(peer_id, Default::default());
                }

                if swarm
                    .behaviour_mut()
                    .broadcast_event_sender
                    .send(BroadcastableNetworkEvent::ConnectionEstablished {
                        peer: peer_id,
                        endpoint: endpoint.clone(),
                    })
                    .is_err()
                {
                    log::warn!("Error sending BroadcastableNetworkEvent::ConnectionEstablished {{ {peer_id} }}");
                }
            }
        }
        SwarmEvent::ConnectionClosed {
            peer_id,
            endpoint,
            num_established,
            cause,
        } => {
            info!(
                "SwarmEvent::ConnectionClosed {} {:?} {} {:?}",
                peer_id, endpoint, num_established, cause
            );
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error } => {
            info!(
                "SwarmEvent::OutgoingConnectionError {:?}: {}",
                peer_id, error
            );
            // Note: if the outgoing connection is to a rendezvous we probably want to keep retrying
            if let Some(peer) = &peer_id {
                if let DialError::Transport(errors) = error {
                    for (address, transport_error) in &errors {
                        // TODO: probably blacklist the address
                        log::debug!("Removing {address} from {peer} because: {transport_error:?}");
                        swarm.behaviour_mut().kademlia.remove_address(peer, address);
                    }
                }
            }
        }
        SwarmEvent::Behaviour(behaviour) => match behaviour {
            ComposedEvent::SimpleFile(event) => simple_file_exchange::handle_event(event, swarm),
            ComposedEvent::MediaStreaming(event) => media_streaming::handle_event(event, swarm),
            ComposedEvent::PaymentInfo(event) => payment_info::handlers::handle_event(event, swarm),
            ComposedEvent::Kademlia(event) => handle_kademlia_event(event, swarm),
            // ComposedEvent::Mdns(event) => handle_mdns_event(event, swarm),
            ComposedEvent::RendezvousServer(event) => handle_rendezvous_server(event),
            ComposedEvent::RendezvousClient(event) => handle_rendezvous_client(event, swarm),
            ComposedEvent::Identify(event) => handle_identity(event, opt, swarm),
            ComposedEvent::Ping(event) => handle_ping(event, swarm),
            ComposedEvent::Gossip(event) => gossip::handle_gossip(event, swarm),
            ComposedEvent::Relay(event) => relay_server::handle(event, swarm),
            ComposedEvent::Dcutr(event) => dcutr_behaviour::handle(event, swarm),
            ComposedEvent::WebRtcSignaling(event) => webrtc_signaling::handle_event(event, swarm),
            ComposedEvent::DelegatedStreaming(event) => {
                delegated_streaming::handle_event(event, swarm)
            }
        },
        SwarmEvent::BannedPeer { peer_id, endpoint } => {
            info!("SwarmEvent::BannedPeer {} at {:?}", peer_id, endpoint);
        }
        SwarmEvent::ExpiredListenAddr {
            listener_id,
            address,
        } => {
            info!(
                "SwarmEvent::ExpiredListenAddr {:?} at {}",
                listener_id, address
            );
        }
        SwarmEvent::ListenerClosed {
            listener_id,
            addresses,
            reason,
        } => {
            info!(
                "SwarmEvent::ListenerClosed {:?} at {:?}: {:?}",
                listener_id, addresses, reason
            );
        }
        SwarmEvent::ListenerError { listener_id, error } => {
            warn!("SwarmEvent::ListenerError {:?}: {}", listener_id, error);
        }
        SwarmEvent::Dialing(t) => {
            debug!("SwarmEvent::Dialing {}", t);
        }
    }
}

pub fn handle_discover_tick(opt: &Configuration, swarm: &mut Swarm<ComposedBehaviour>) {
    if !opt.disable_rendezvous_discover {
        let has_any = swarm
            .behaviour()
            .state
            .rendezvous_peers
            .values()
            .any(|v| v.cookie.is_some());
        if has_any {
            let rendezvous_peers = swarm.behaviour().state.rendezvous_peers.clone();
            for (peer, state) in rendezvous_peers {
                if let Some(cookie) = state.cookie {
                    if swarm.is_connected(&peer) {
                        log::debug!(
                            "handle_discover_tick rendezvous_client.discover {cookie:?} {peer}"
                        );
                        swarm.behaviour_mut().rendezvous_client.discover(
                            Some(rendezvous::Namespace::from_static(RENDEZVOUS_NAMESPACE)),
                            Some(cookie),
                            None,
                            peer,
                        );
                    } else {
                        match swarm.dial(peer) {
                            Ok(_) => log::info!("(re)dialing rendezvous {peer}"),
                            Err(e) => log::warn!("Error (re)dialing rendezvous {peer}: {e}"),
                            //TODO: perhaps find new rendezvous
                        }
                    }
                }
            }
        } else {
            let data = &swarm.behaviour().state.rendezvous_peers;
            log::debug!("handle_discover_tick got empty {data:?}");
            //TODO: perhaps (re)dial original address or find new rendezvous
        }
    }
}

pub enum StartProvidingAction {
    SimpleFile {
        filename: String,
        sender: oneshot::Sender<Result<(), libp2p::kad::AddProviderError>>,
    },
    MediaStreaming {
        sender: oneshot::Sender<Result<(), libp2p::kad::AddProviderError>>,
    },
}

// #[derive(Debug)]
pub enum ComposedEvent {
    SimpleFile(RequestResponseEvent<SimpleFileRequest, Result<SimpleFileResponse, String>>),
    MediaStreaming(RequestResponseEvent<StreamingRequest, WrappedStreamingResponseResult>),
    PaymentInfo(RequestResponseEvent<PaymentInfoRequest, PaymentInfoResponseResult>),
    WebRtcSignaling(
        RequestResponseEvent<SignalingRequestOrResponse, WebRtcSignalingResponseResult>,
    ),
    DelegatedStreaming(
        RequestResponseEvent<DelegatedStreamingRequest, DelegatedStreamingResponseResult>,
    ),
    Kademlia(KademliaEvent),
    // Mdns(MdnsEvent),
    Identify(IdentifyEvent),
    Ping(ping::Event),
    RendezvousServer(rendezvous::server::Event),
    RendezvousClient(rendezvous::client::Event),
    // Gossip(gossipsub::GossipsubEvent),
    Gossip(floodsub::FloodsubEvent),
    Relay(relay::Event),
    Dcutr(dcutr::behaviour::Event),
}

impl From<RequestResponseEvent<SimpleFileRequest, Result<SimpleFileResponse, String>>>
    for ComposedEvent
{
    fn from(
        event: RequestResponseEvent<SimpleFileRequest, Result<SimpleFileResponse, String>>,
    ) -> Self {
        ComposedEvent::SimpleFile(event)
    }
}

impl From<RequestResponseEvent<StreamingRequest, WrappedStreamingResponseResult>>
    for ComposedEvent
{
    fn from(event: RequestResponseEvent<StreamingRequest, WrappedStreamingResponseResult>) -> Self {
        ComposedEvent::MediaStreaming(event)
    }
}

impl From<RequestResponseEvent<PaymentInfoRequest, PaymentInfoResponseResult>> for ComposedEvent {
    fn from(event: RequestResponseEvent<PaymentInfoRequest, PaymentInfoResponseResult>) -> Self {
        ComposedEvent::PaymentInfo(event)
    }
}

impl From<RequestResponseEvent<SignalingRequestOrResponse, WebRtcSignalingResponseResult>>
    for ComposedEvent
{
    fn from(
        event: RequestResponseEvent<SignalingRequestOrResponse, WebRtcSignalingResponseResult>,
    ) -> Self {
        ComposedEvent::WebRtcSignaling(event)
    }
}

impl From<RequestResponseEvent<DelegatedStreamingRequest, DelegatedStreamingResponseResult>>
    for ComposedEvent
{
    fn from(
        event: RequestResponseEvent<DelegatedStreamingRequest, DelegatedStreamingResponseResult>,
    ) -> Self {
        ComposedEvent::DelegatedStreaming(event)
    }
}

impl From<KademliaEvent> for ComposedEvent {
    fn from(event: KademliaEvent) -> Self {
        ComposedEvent::Kademlia(event)
    }
}

// impl From<MdnsEvent> for ComposedEvent {
//     fn from(event: MdnsEvent) -> Self {
//         ComposedEvent::Mdns(event)
//     }
// }

impl From<rendezvous::server::Event> for ComposedEvent {
    fn from(event: rendezvous::server::Event) -> Self {
        ComposedEvent::RendezvousServer(event)
    }
}

impl From<rendezvous::client::Event> for ComposedEvent {
    fn from(event: rendezvous::client::Event) -> Self {
        ComposedEvent::RendezvousClient(event)
    }
}

impl From<IdentifyEvent> for ComposedEvent {
    fn from(event: IdentifyEvent) -> Self {
        ComposedEvent::Identify(event)
    }
}

impl From<ping::Event> for ComposedEvent {
    fn from(event: ping::Event) -> Self {
        ComposedEvent::Ping(event)
    }
}

impl From<floodsub::FloodsubEvent> for ComposedEvent {
    fn from(event: floodsub::FloodsubEvent) -> Self {
        ComposedEvent::Gossip(event)
    }
}

impl From<relay::Event> for ComposedEvent {
    fn from(event: relay::Event) -> Self {
        ComposedEvent::Relay(event)
    }
}

impl From<dcutr::behaviour::Event> for ComposedEvent {
    fn from(e: dcutr::behaviour::Event) -> Self {
        ComposedEvent::Dcutr(e)
    }
}

#[derive(Default, Debug, Clone)]
pub struct RendezvousState {
    cookie: Option<Cookie>,
}

#[derive(Default)]
pub struct NetworkState {
    pending_start_providing: HashMap<libp2p::kad::QueryId, StartProvidingAction>,
    pending_get_providers: HashMap<
        libp2p::kad::QueryId,
        oneshot::Sender<Result<HashSet<libp2p::PeerId>, GetProvidersError>>,
    >,
    pending_get_records: HashMap<Key, Vec<oneshot::Sender<anyhow::Result<KademliaRecord>>>>,
    cached_kademlia_records: HashMap<Key, KademliaRecord>,
    pending_put_records:
        HashMap<libp2p::kad::QueryId, (KademliaRecord, oneshot::Sender<anyhow::Result<()>>)>,
    pending_simple_file_request: HashMap<
        libp2p::request_response::RequestId,
        oneshot::Sender<
            Result<Result<SimpleFileResponse, String>, libp2p::request_response::OutboundFailure>,
        >,
    >,
    pending_streaming_request: HashMap<
        libp2p::request_response::RequestId,
        oneshot::Sender<Result<StreamingResponseResult, libp2p::request_response::OutboundFailure>>,
    >,
    pending_payment_info_request: HashMap<
        libp2p::request_response::RequestId,
        oneshot::Sender<
            Result<PaymentInfoResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    >,
    pending_webrtc_signaling_request: HashMap<
        libp2p::request_response::RequestId,
        oneshot::Sender<
            Result<WebRtcSignalingResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    >,
    pending_delegated_streaming_request: HashMap<
        libp2p::request_response::RequestId,
        oneshot::Sender<
            Result<DelegatedStreamingResponseResult, libp2p::request_response::OutboundFailure>,
        >,
    >,
    published_files_mapping: HashMap<libp2p::kad::record::Key, String>,
    rendezvous_peers: HashMap<PeerId, RendezvousState>,
    peer_statistics: HashMap<PeerId, PeerStatistics>,
}

#[derive(Debug, Clone, Default)]
pub struct PeerStatistics {
    pub latency: Option<Duration>,
}

#[derive(libp2p::NetworkBehaviour)]
#[behaviour(out_event = "ComposedEvent")]
pub struct ComposedBehaviour {
    pub simple_file_exchange: RequestResponse<simple_file_exchange::SimpleFileExchangeCodec>,
    pub media_streaming: RequestResponse<media_streaming::StreamingCodec>,
    pub webrtc_signaling: RequestResponse<webrtc_signaling::WebRtcSignalingCodec>,
    pub delegated_streaming: RequestResponse<delegated_streaming::DelegatedStreamingCodec>,
    pub payment_info: RequestResponse<payment_info::PaymentInfoCodec>,
    pub kademlia: Kademlia<MemoryStore>,
    // mdns: Mdns,
    pub identify: Identify,
    pub ping: libp2p::ping::Ping,
    pub rendezvous_server: rendezvous::server::Behaviour,
    pub rendezvous_client: rendezvous::client::Behaviour,
    // pub gossip: gossipsub::Gossipsub,
    pub gossip: floodsub::Floodsub,
    pub relay_server: relay::Relay,
    pub dcutr: dcutr::behaviour::Behaviour,
    #[behaviour(ignore)]
    pub state: NetworkState,
    #[behaviour(ignore)]
    pub event_sender: mpsc::UnboundedSender<InternalNetworkEvent>,
    #[behaviour(ignore)]
    pub broadcast_event_sender: broadcast::Sender<BroadcastableNetworkEvent>,
    #[behaviour(ignore)]
    pub webrtc_signaling_sender: mpsc::UnboundedSender<webrtc_signaling::RequestEvent>,
    #[behaviour(ignore)]
    pub delegated_streaming_sender: mpsc::UnboundedSender<delegated_streaming::RequestEvent>,
}

#[derive(Clone)]
pub struct NodeIdentity {
    pub keypair: Keypair,
    pub peer_id: PeerId,
    pub org_keypair: Keypair,
}
