// SPDX-License-Identifier: AGPL-3.0-only


// use libp2p::gossipsub::{self, MessageAuthenticity, ValidationMode};

use libp2p::identity::{self};
use libp2p::PeerId;


pub async fn our_transport(
    keypair: &identity::Keypair,
) -> std::io::Result<libp2p::core::transport::Boxed<(PeerId, libp2p::core::muxing::StreamMuxerBox)>>
{
    use libp2p::Transport;

    let transport = {
        let tcp = libp2p::tcp::TokioTcpConfig::new().nodelay(true);
        let dns_tcp = libp2p::dns::TokioDnsConfig::system(tcp)?;
        let ws_dns_tcp = libp2p::websocket::WsConfig::new(dns_tcp.clone());
        dns_tcp.or_transport(ws_dns_tcp)
    };

    let noise_keys = libp2p::noise::Keypair::<libp2p::noise::X25519Spec>::new()
        .into_authentic(keypair)
        .expect("Signing libp2p-noise static DH keypair failed.");

    let mplex_config = {
        let mut m = libp2p::mplex::MplexConfig::default();
        m.set_max_buffer_size(128);
        m
    };

    Ok(transport
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(libp2p::noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(mplex_config)
        .timeout(std::time::Duration::from_secs(20))
        .boxed())
}
