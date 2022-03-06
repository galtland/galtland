// SPDX-License-Identifier: AGPL-3.0-only

// TODO: add gossip again after https://github.com/libp2p/rust-libp2p/issues/2497 has been solved
// use libp2p::gossipsub::{GossipsubEvent, IdentTopic, Topic};

use libp2p::floodsub::{FloodsubEvent, Topic};
use libp2p::Swarm;

use super::ComposedBehaviour;
use crate::daemons::internal_network_events::BroadcastableNetworkEvent;


// pub(crate) fn rtmp_keys_gossip() -> IdentTopic {
//     Topic::new("new_rtmp_keys")
// }

// pub(crate) fn handle_gossip(event: SubEvent, swarm: &mut Swarm<ComposedBehaviour>) {
//     match event {
//         SubEvent::Message {
//             propagation_source,
//             message_id,
//             message,
//         } => {
//             log::debug!("GossipsubEvent::Message {propagation_source} {message_id} {message:?}");
//             if let Err(e) = swarm
//                 .behaviour_mut()
//                 .event_sender
//                 .send(InternalNetworkEvent::ReceivedGossip { message })
//             {
//                 log::warn!("Error sending ReceivedGossip: {}", e);
//             }
//         }
//         GossipsubEvent::Subscribed { peer_id, topic } => {
//             log::debug!("GossipsubEvent::Subscribed {peer_id} {topic}")
//         }
//         GossipsubEvent::Unsubscribed { peer_id, topic } => {
//             log::debug!("GossipsubEvent::Unsubscribed {peer_id} {topic}")
//         }
//         GossipsubEvent::GossipsubNotSupported { peer_id } => {
//             log::debug!("GossipsubEvent::GossipsubNotSupported {peer_id}")
//         }
//     }
// }


pub(crate) fn rtmp_keys_gossip() -> Topic {
    Topic::new("new_rtmp_keys")
}

pub(crate) fn handle_gossip(event: FloodsubEvent, swarm: &mut Swarm<ComposedBehaviour>) {
    match event {
        FloodsubEvent::Message(message) => {
            log::debug!("FloodsubEvent::Message {message:?}");
            if let Err(e) = swarm
                .behaviour_mut()
                .broadcast_event_sender
                .send(BroadcastableNetworkEvent::ReceivedGossip { message })
            {
                log::warn!("Error sending ReceivedGossip: {}", e);
            }
        }
        FloodsubEvent::Subscribed { peer_id, topic } => {
            log::debug!("FloodsubEvent::Subscribed {peer_id} {topic:?}")
        }
        FloodsubEvent::Unsubscribed { peer_id, topic } => {
            log::debug!("FloodsubEvent::Unsubscribed {peer_id} {topic:?}")
        }
    }
}
