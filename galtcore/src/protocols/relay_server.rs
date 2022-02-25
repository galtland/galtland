// SPDX-License-Identifier: AGPL-3.0-only


use libp2p::relay::v2::relay;
use libp2p::Swarm;

use super::ComposedBehaviour;


pub fn handle(event: relay::Event, _swarm: &mut Swarm<ComposedBehaviour>) {
    match event {
        relay::Event::ReservationReqAccepted {
            src_peer_id,
            renewed,
        } => {
            log::debug!("relay::Event::ReservationReqAccepted {src_peer_id} {renewed}")
        }
        relay::Event::ReservationReqAcceptFailed { src_peer_id, error } => {
            log::debug!("relay::Event::ReservationReqAcceptFailed {src_peer_id} {error}")
        }
        relay::Event::ReservationReqDenied { src_peer_id } => {
            log::debug!("relay::Event::ReservationReqDenied {src_peer_id}")
        }
        relay::Event::ReservationReqDenyFailed { src_peer_id, error } => {
            log::debug!("relay::Event::ReservationReqDenyFailed {src_peer_id} {error}")
        }
        relay::Event::ReservationTimedOut { src_peer_id } => {
            log::debug!("relay::Event::ReservationTimedOut {src_peer_id}")
        }
        relay::Event::CircuitReqReceiveFailed { src_peer_id, error } => {
            log::debug!("relay::Event::CircuitReqReceiveFailed {src_peer_id} {error}")
        }
        relay::Event::CircuitReqDenied {
            src_peer_id,
            dst_peer_id,
        } => {
            log::debug!("relay::Event::CircuitReqDenied {src_peer_id} {dst_peer_id}")
        }
        relay::Event::CircuitReqDenyFailed {
            src_peer_id,
            dst_peer_id,
            error,
        } => {
            log::debug!("relay::Event::CircuitReqDenyFailed {src_peer_id} {dst_peer_id} {error}")
        }
        relay::Event::CircuitReqAccepted {
            src_peer_id,
            dst_peer_id,
        } => {
            log::debug!("relay::Event::CircuitReqAccepted {src_peer_id} {dst_peer_id}")
        }
        relay::Event::CircuitReqOutboundConnectFailed {
            src_peer_id,
            dst_peer_id,
            error,
        } => {
            log::debug!(
                "relay::Event::CircuitReqOutboundConnectFailed {src_peer_id} {dst_peer_id} {error}"
            )
        }
        relay::Event::CircuitReqAcceptFailed {
            src_peer_id,
            dst_peer_id,
            error,
        } => {
            log::debug!("relay::Event::CircuitReqAcceptFailed {src_peer_id} {dst_peer_id} {error}")
        }
        relay::Event::CircuitClosed {
            src_peer_id,
            dst_peer_id,
            error,
        } => {
            log::debug!("relay::Event::CircuitClosed {src_peer_id} {dst_peer_id} {error:?}")
        }
    }
}
