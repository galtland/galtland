// SPDX-License-Identifier: AGPL-3.0-only

use libp2p::Multiaddr;

pub fn get_official_rendezvous() -> Vec<Multiaddr> {
    [
        "/dns/eu.galtland.network/tcp/60555",
        "/dns/sa.galtland.network/tcp/60555",
        // "/ip4/127.0.0.1/tcp/60555"
    ]
    .into_iter()
    .map(|v| v.parse().expect("to be fine"))
    .collect()
}

pub fn get_official_rendezvous_ws() -> Vec<Multiaddr> {
    [
        "/dns/eu.galtland.network/tcp/60556/ws",
        "/dns/sa.galtland.network/tcp/60556/ws",
        // "/ip4/127.0.0.1/tcp/60556/ws"
    ]
    .into_iter()
    .map(|v| v.parse().expect("to be fine"))
    .collect()
}
