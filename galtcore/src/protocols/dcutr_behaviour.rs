// SPDX-License-Identifier: AGPL-3.0-only


use libp2p::{dcutr, Swarm};

use super::ComposedBehaviour;


pub(crate) fn handle(event: dcutr::behaviour::Event, _swarm: &mut Swarm<ComposedBehaviour>) {
    match event {
        dcutr::behaviour::Event::InitiatedDirectConnectionUpgrade {
            remote_peer_id,
            local_relayed_addr,
        } => log::debug!("dcutr::behaviour::Event::InitiatedDirectConnectionUpgrade {remote_peer_id} {local_relayed_addr}"),
        dcutr::behaviour::Event::RemoteInitiatedDirectConnectionUpgrade {
            remote_peer_id,
            remote_relayed_addr,
        } => log::debug!("dcutr::behaviour::Event::RemoteInitiatedDirectConnectionUpgrade {remote_peer_id} {remote_relayed_addr}"),
        dcutr::behaviour::Event::DirectConnectionUpgradeSucceeded { remote_peer_id } => {
            log::debug!("dcutr::behaviour::Event::DirectConnectionUpgradeSucceeded {remote_peer_id}")
        }
        dcutr::behaviour::Event::DirectConnectionUpgradeFailed {
            remote_peer_id,
            error,
        } => log::debug!("dcutr::behaviour::Event::DirectConnectionUpgradeFailed {remote_peer_id} {error}"),
    }
}
