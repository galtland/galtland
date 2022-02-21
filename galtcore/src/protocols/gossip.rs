use libp2p::gossipsub::{GossipsubEvent, IdentTopic, Topic};
use libp2p::Swarm;

use super::ComposedBehaviour;
use crate::protocols::InternalNetworkEvent;

pub fn rtmp_keys_gossip() -> IdentTopic {
    Topic::new("new_rtmp_keys")
}

pub fn handle_gossip(event: GossipsubEvent, swarm: &mut Swarm<ComposedBehaviour>) {
    match event {
        GossipsubEvent::Message {
            propagation_source,
            message_id,
            message,
        } => {
            log::debug!("GossipsubEvent::Message {propagation_source} {message_id} {message:?}");
            if let Err(e) = swarm
                .behaviour_mut()
                .event_sender
                .send(InternalNetworkEvent::ReceivedGossip { message })
            {
                log::warn!("Error sending ReceivedGossip: {}", e);
            }
        }
        GossipsubEvent::Subscribed { peer_id, topic } => {
            log::debug!("GossipsubEvent::Subscribed {peer_id} {topic}")
        }
        GossipsubEvent::Unsubscribed { peer_id, topic } => {
            log::debug!("GossipsubEvent::Unsubscribed {peer_id} {topic}")
        }
        GossipsubEvent::GossipsubNotSupported { peer_id } => {
            log::debug!("GossipsubEvent::GossipsubNotSupported {peer_id}")
        }
    }
}
