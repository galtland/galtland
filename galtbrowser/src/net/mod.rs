use std::collections::HashSet;
use std::time::Duration;

use galtcore::configuration::Configuration;
use galtcore::daemons::gossip_listener::GossipListenerClient;
use galtcore::tokio::sync::{broadcast, mpsc};
use galtcore::{daemons, networkbackendclient, peer_seeds, protocols, tokio};
use libp2p::futures::StreamExt;
use libp2p::identity::{self};
use libp2p::Multiaddr;

use crate::transport;

pub mod webrtc;


pub(crate) async fn start_websockets() -> anyhow::Result<webrtc::state::WebRtcState> {
    let keypair = identity::Keypair::generate_ed25519();

    let my_peer_id = keypair.public().to_peer_id();

    log::info!("My public peer id is {}", my_peer_id);
    let identity = protocols::NodeIdentity {
        keypair: keypair.clone(),
        peer_id: my_peer_id,
    };

    let rendezvous_addresses: HashSet<Multiaddr> = peer_seeds::get_official_rendezvous_ws()
        .into_iter()
        .collect();

    let configuration = Configuration {
        disable_streaming: true,
        disable_rendezvous_register: true,
        disable_rendezvous_discover: false,
        max_bytes_per_second_upload_stream: None,
        rendezvous_addresses: rendezvous_addresses.clone(),
    };
    let (network_backend_command_sender, mut network_backend_command_receiver) = mpsc::channel(10);

    let (highlevel_command_sender, highlevel_command_receiver) = mpsc::channel(10);

    let networkbackendclient =
        networkbackendclient::NetworkBackendClient::new(network_backend_command_sender);

    let transport = transport::our_transport(&keypair).expect("to build protocol");
    let (event_sender, event_receiver) = mpsc::unbounded_channel();
    let (broadcast_event_sender, broadcast_event_receiver) = broadcast::channel(10);
    let (webrtc_signaling_sender, webrtc_signaling_receiver) = mpsc::unbounded_channel();

    let mut swarm = galtcore::swarm::build(
        configuration.clone(),
        keypair,
        event_sender,
        broadcast_event_sender.clone(),
        webrtc_signaling_sender,
        transport,
    )
    .await;

    for address in rendezvous_addresses {
        match swarm.dial(address.clone()) {
            Ok(_) => log::info!("Dialing rendezvous point address {}", address),
            Err(e) => log::warn!("Failed to dial {}: {}", address, e),
        }
    }

    galtcore::utils::spawn_and_log_error(daemons::cm::run_loop(
        configuration.clone(),
        networkbackendclient.clone(),
        highlevel_command_receiver,
        identity,
    ));

    let gossip_listener_client = GossipListenerClient::new(networkbackendclient.clone());

    galtcore::utils::spawn_and_log_error(daemons::internal_network_events::run_loop(
        event_receiver,
        broadcast_event_receiver,
        highlevel_command_sender.clone(),
        gossip_listener_client,
    ));

    let webrtc_state: webrtc::state::WebRtcState = Default::default();
    galtcore::utils::wspawn_and_log_error({
        let broadcast_event_receiver = broadcast_event_sender.subscribe();
        let webrtc_state = webrtc_state.clone();
        async move {
            webrtc_state
                .run_receive_loop(
                    broadcast_event_receiver,
                    webrtc_signaling_receiver,
                    networkbackendclient,
                )
                .await
        }
    });

    let mut discover_tick = wasm_timer::Interval::new(Duration::from_secs(30));
    galtcore::utils::spawn_and_log_error(async move {
        loop {
            log::trace!("Looping main loop");
            tokio::task::yield_now().await;
            tokio::select! {
                _ = discover_tick.next() => {
                    protocols::handle_discover_tick(&configuration, &mut swarm)
                },
                command = network_backend_command_receiver.recv() => match command {
                    Some(e) => protocols::handle_network_backend_command(e, &mut swarm).expect("to handle command"),
                    None => todo!(),
                },
                event = swarm.select_next_some() => protocols::handle_swarm_event(event, &configuration, &mut swarm)
            };
        }
    });
    Ok(webrtc_state)
}
