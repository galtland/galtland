use std::time::Duration;

use libp2p::gossipsub::{self, MessageAuthenticity, ValidationMode};
use libp2p::identify::{Identify, IdentifyConfig};
use libp2p::identity::{self};
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaConfig, KademliaStoreInserts};
use libp2p::rendezvous::{self};
use libp2p::request_response::{ProtocolSupport, RequestResponse, RequestResponseConfig};
use libp2p::swarm::SwarmBuilder;
use libp2p::{ping, PeerId, Swarm};

use super::protocols::payment_info::{PaymentInfoCodec, PaymentInfoProtocol};
use super::protocols::rtmp_streaming::{RTMPStreamingCodec, RTMPStreamingProtocol};
use super::protocols::simple_file_exchange::{SimpleFileExchangeCodec, SimpleFileExchangeProtocol};
use super::protocols::{self, ComposedBehaviour};
use crate::configuration::Configuration;
use crate::daemons::internal_network_events::InternalNetworkEvent;

pub async fn build(
    configuration: Configuration,
    keypair: identity::Keypair,
    event_sender: tokio::sync::mpsc::UnboundedSender<InternalNetworkEvent>,
) -> Swarm<ComposedBehaviour> {
    let my_peer_id = keypair.public().to_peer_id();

    let rr_cfg = {
        let mut c = RequestResponseConfig::default();
        c.set_connection_keep_alive(Duration::from_secs(20))
            .set_request_timeout(Duration::from_secs(20));
        c
    };
    let kademlia_config = {
        let mut c = KademliaConfig::default();
        c.set_record_filtering(KademliaStoreInserts::FilterBoth);
        c
    };

    let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
        .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
        .message_id_fn(|m| m.data.to_vec().into()) // content-address messages. No two messages of the
        // same content will be propagated.
        .build()
        .expect("Valid config");
    let mut gossip = gossipsub::Gossipsub::new(
        MessageAuthenticity::Signed(keypair.clone()),
        gossipsub_config,
    )
    .expect("Correct configuration");

    gossip
        .subscribe(&protocols::gossip::rtmp_keys_gossip())
        .expect("to work");

    let rtmp_streaming_protocol_support = if configuration.disable_streaming {
        ProtocolSupport::Outbound // Won't be able to receive streaming requests
    } else {
        ProtocolSupport::Full
    };

    SwarmBuilder::new(
        our_transport(keypair.clone()).await.unwrap(),
        protocols::ComposedBehaviour {
            simple_file_exchange: RequestResponse::new(
                SimpleFileExchangeCodec(),
                std::iter::once((SimpleFileExchangeProtocol(), ProtocolSupport::Full)),
                rr_cfg.clone(),
            ),
            rtmp_streaming: RequestResponse::new(
                RTMPStreamingCodec {
                    event_sender: event_sender.clone(),
                },
                std::iter::once((RTMPStreamingProtocol(), rtmp_streaming_protocol_support)),
                rr_cfg.clone(),
            ),
            payment_info: RequestResponse::new(
                PaymentInfoCodec {},
                std::iter::once((PaymentInfoProtocol(), ProtocolSupport::Full)),
                rr_cfg,
            ),
            kademlia: Kademlia::with_config(
                my_peer_id,
                MemoryStore::new(my_peer_id),
                kademlia_config,
            ),
            // mdns: Mdns::new(MdnsConfig::default()).await.unwrap(),
            identify: Identify::new(IdentifyConfig::new(
                "galtland/1.0.0".to_string(),
                keypair.public(),
            )),
            ping: ping::Behaviour::new(ping::Config::new().with_keep_alive(true)),
            rendezvous_server: rendezvous::server::Behaviour::new(
                rendezvous::server::Config::default(),
            ),
            rendezvous_client: rendezvous::client::Behaviour::new(keypair),
            state: Default::default(),
            event_sender,
            gossip,
        },
        my_peer_id,
    )
    // We want the connection background tasks to be spawned
    // onto the tokio runtime.
    .executor(Box::new(|fut| {
        tokio::spawn(fut);
    }))
    .build()
}

async fn our_transport(
    keypair: identity::Keypair,
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
        .into_authentic(&keypair)
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
