// SPDX-License-Identifier: AGPL-3.0-only

use std::time::Duration;

// use libp2p::gossipsub::{self, MessageAuthenticity, ValidationMode};
use libp2p::identify::{Identify, IdentifyConfig};
use libp2p::kad::store::MemoryStore;
use libp2p::kad::{Kademlia, KademliaConfig, KademliaStoreInserts};
use libp2p::relay::v2::relay;
use libp2p::rendezvous::{self};
use libp2p::request_response::{ProtocolSupport, RequestResponse, RequestResponseConfig};
use libp2p::swarm::SwarmBuilder;
use libp2p::{dcutr, floodsub, ping, PeerId, Swarm};

use crate::configuration::Configuration;
use crate::daemons::internal_network_events::{BroadcastableNetworkEvent, InternalNetworkEvent};
use crate::protocols::delegated_streaming::{
    self, DelegatedStreamingCodec, DelegatedStreamingProtocol,
};
use crate::protocols::galt_identify::{GaltIdentifyCodec, GaltIdentifyProtocol};
use crate::protocols::media_streaming::{StreamingCodec, StreamingProtocol};
use crate::protocols::payment_info::{PaymentInfoCodec, PaymentInfoProtocol};
use crate::protocols::simple_file_exchange::{SimpleFileExchangeCodec, SimpleFileExchangeProtocol};
use crate::protocols::webrtc_signaling::{WebRtcSignalingCodec, WebRtcSignalingProtocol};
use crate::protocols::{self, webrtc_signaling, ComposedBehaviour, NodeIdentity};
use crate::utils;

type OurTransport = libp2p::core::transport::Boxed<(PeerId, libp2p::core::muxing::StreamMuxerBox)>;

pub async fn build(
    configuration: Configuration,
    identity: NodeIdentity,
    event_sender: tokio::sync::mpsc::UnboundedSender<InternalNetworkEvent>,
    broadcast_event_sender: tokio::sync::broadcast::Sender<BroadcastableNetworkEvent>,
    webrtc_signaling_sender: tokio::sync::mpsc::UnboundedSender<webrtc_signaling::RequestEvent>,
    delegated_streaming_sender: tokio::sync::mpsc::UnboundedSender<
        delegated_streaming::RequestEvent,
    >,
    transport: OurTransport,
) -> Swarm<ComposedBehaviour> {
    let my_peer_id = identity.peer_id;

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

    // let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
    //     .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
    //     .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
    //     .message_id_fn(|m| m.data.to_vec().into()) // content-address messages. No two messages of the
    //     // same content will be propagated.
    //     .build()
    //     .expect("Valid config");
    // let mut gossip = gossipsub::Gossipsub::new(
    //     MessageAuthenticity::Signed(keypair.clone()),
    //     gossipsub_config,
    // )
    // .expect("Correct configuration");

    let mut gossip = floodsub::Floodsub::new(my_peer_id);

    // gossip
    //     .subscribe(&protocols::gossip::streaming_keys_gossip())
    //     .expect("to work");
    gossip.subscribe(protocols::gossip::streaming_keys_gossip());

    let streaming_protocol_support = if configuration.disable_streaming {
        ProtocolSupport::Outbound // Won't be able to receive streaming requests
    } else {
        ProtocolSupport::Full
    };

    SwarmBuilder::new(
        transport,
        protocols::ComposedBehaviour {
            simple_file_exchange: RequestResponse::new(
                SimpleFileExchangeCodec(),
                std::iter::once((SimpleFileExchangeProtocol(), ProtocolSupport::Full)),
                rr_cfg.clone(),
            ),
            media_streaming: RequestResponse::new(
                StreamingCodec {
                    broadcast_event_sender: broadcast_event_sender.clone(),
                },
                std::iter::once((StreamingProtocol(), streaming_protocol_support)),
                rr_cfg.clone(),
            ),
            webrtc_signaling: RequestResponse::new(
                WebRtcSignalingCodec {},
                std::iter::once((WebRtcSignalingProtocol(), ProtocolSupport::Full)),
                rr_cfg.clone(),
            ),
            delegated_streaming: RequestResponse::new(
                DelegatedStreamingCodec {},
                std::iter::once((DelegatedStreamingProtocol(), ProtocolSupport::Full)),
                rr_cfg.clone(),
            ),
            galt_identify: RequestResponse::new(
                GaltIdentifyCodec {},
                std::iter::once((GaltIdentifyProtocol(), ProtocolSupport::Full)),
                rr_cfg.clone(),
            ),
            payment_info: RequestResponse::new(
                PaymentInfoCodec {},
                std::iter::once((PaymentInfoProtocol(), ProtocolSupport::Full)),
                rr_cfg,
            ),
            // mdns: Mdns::new(MdnsConfig::default()).await.unwrap(),
            kademlia: Kademlia::with_config(
                my_peer_id,
                MemoryStore::new(my_peer_id),
                kademlia_config,
            ),
            identify: Identify::new(IdentifyConfig::new(
                "galtland/1.0.0".to_string(),
                identity.keypair.public(),
            )),
            ping: ping::Behaviour::new(ping::Config::new().with_keep_alive(true)),
            rendezvous_server: rendezvous::server::Behaviour::new(
                rendezvous::server::Config::default(),
            ),
            rendezvous_client: rendezvous::client::Behaviour::new(
                identity.keypair.as_ref().to_owned(),
            ),
            gossip,
            relay_server: relay::Relay::new(my_peer_id, Default::default()),
            dcutr: dcutr::behaviour::Behaviour::new(),
            state: Default::default(),
            event_sender,
            broadcast_event_sender,
            webrtc_signaling_sender,
            delegated_streaming_sender,
        },
        my_peer_id,
    )
    .executor(Box::new(|fut| {
        utils::spawn(fut);
    }))
    .build()
}
