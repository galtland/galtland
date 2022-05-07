// SPDX-License-Identifier: AGPL-3.0-only


use libp2p::relay::v2::client;
use libp2p::Swarm;

use super::ComposedBehaviour;


pub(crate) fn handle(event: client::Event, _swarm: &mut Swarm<ComposedBehaviour>) {
    match event {
        client::Event::ReservationReqAccepted {
            relay_peer_id,
            renewal,
            limit,
        } => log::info!(
            "relay client::Event::ReservationReqAccepted {relay_peer_id} {renewal} {limit:?}"
        ),

        client::Event::ReservationReqFailed {
            relay_peer_id,
            renewal,
            error,
        } => log::info!(
            "relay client::Event::ReservationReqFailed {relay_peer_id} {renewal} {error:?}"
        ),

        client::Event::OutboundCircuitEstablished {
            relay_peer_id,
            limit,
        } => {
            log::info!("relay client::Event::OutboundCircuitEstablished {relay_peer_id} {limit:?}")
        }

        client::Event::OutboundCircuitReqFailed {
            relay_peer_id,
            error,
        } => log::info!("relay client::Event::OutboundCircuitReqFailed {relay_peer_id} {error:?}"),

        client::Event::InboundCircuitEstablished { src_peer_id, limit } => {
            log::info!("relay client::Event::InboundCircuitEstablished {src_peer_id} {limit:?}")
        }

        client::Event::InboundCircuitReqFailed {
            relay_peer_id,
            error,
        } => log::info!("relay client::Event::InboundCircuitReqFailed {relay_peer_id} {error:?}"),

        client::Event::InboundCircuitReqDenied { src_peer_id } => {
            log::info!("relay client::Event::InboundCircuitReqDenied {src_peer_id}")
        }

        client::Event::InboundCircuitReqDenyFailed { src_peer_id, error } => {
            log::info!("relay client::Event::InboundCircuitReqDenyFailed {src_peer_id} {error:?}")
        }
    }
}
