// SPDX-License-Identifier: AGPL-3.0-only


// use galtcore::libp2p::gossipsub::{self, MessageAuthenticity, ValidationMode};

use galtcore::libp2p::identity::{self};
use galtcore::libp2p::PeerId;


pub async fn our_transport(
    keypair: &identity::Keypair,
) -> std::io::Result<(
    galtcore::libp2p::core::transport::Boxed<(
        PeerId,
        galtcore::libp2p::core::muxing::StreamMuxerBox,
    )>,
    galtcore::libp2p::relay::v2::client::Client,
)> {
    use galtcore::libp2p::Transport;

    let transport = {
        let tcp = galtcore::libp2p::tcp::TokioTcpConfig::new()
            .nodelay(true)
            .port_reuse(true);
        let dns_tcp = galtcore::libp2p::dns::TokioDnsConfig::system(tcp)?;
        let ws_dns_tcp = galtcore::libp2p::websocket::WsConfig::new(dns_tcp.clone());
        dns_tcp.or_transport(ws_dns_tcp)
    };

    let noise_keys = galtcore::libp2p::noise::Keypair::<libp2p::noise::X25519Spec>::new()
        .into_authentic(keypair)
        .expect("Signing galtcore::libp2p-noise static DH keypair failed.");

    // let mplex_config = {
    //     let mut m = galtcore::libp2p::mplex::MplexConfig::default();
    //     m.set_max_buffer_size(128);
    //     m
    // };
    let yamux = galtcore::libp2p::yamux::YamuxConfig::default();

    let (relay_transport, relay_client) =
        galtcore::libp2p::relay::v2::client::Client::new_transport_and_behaviour(
            keypair.public().to_peer_id(),
        );

    let transport = galtcore::libp2p::core::transport::OrTransport::new(relay_transport, transport)
        .upgrade(galtcore::libp2p::core::upgrade::Version::V1)
        .authenticate(libp2p::noise::NoiseConfig::xx(noise_keys).into_authenticated())
        // .multiplex(mplex_config)
        .multiplex(yamux)
        .timeout(std::time::Duration::from_secs(20))
        .boxed();

    Ok((transport, relay_client))
}
