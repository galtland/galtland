use std::collections::HashSet;

use libp2p::Multiaddr;

#[derive(Clone)]
pub struct Configuration {
    pub disable_streaming: bool,
    pub disable_rendezvous_register: bool,
    pub disable_rendezvous_discover: bool,
    pub max_bytes_per_second_upload_stream: Option<byte_unit::Byte>,
    pub rendezvous_addresses: HashSet<Multiaddr>,
}
