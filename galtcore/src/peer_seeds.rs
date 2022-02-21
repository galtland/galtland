// SPDX-License-Identifier: AGPL-3.0-only

use libp2p::Multiaddr;

pub fn get_official_rendezvous() -> Vec<Multiaddr> {
    [
        "/dns/eu.galtland.network/tcp/60555",
        "/dns/sa.galtland.network/tcp/60555",
    ]
    .into_iter()
    .map(|v| v.parse().expect("to be fine"))
    .collect()
}
