use std::collections::HashSet;
use std::time::Duration;

use galtcore::cm::SharedGlobalState;
use galtcore::configuration::Configuration;
use galtcore::daemons::gossip_listener::GossipListenerClient;
use galtcore::tokio::sync::{broadcast, mpsc};
use galtcore::{daemons, networkbackendclient, protocols, tokio};
use libp2p::futures::StreamExt;
use libp2p::identity::{self};
use libp2p::Multiaddr;
use log::info;

use crate::transport;

pub mod webrtc;

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub(crate) enum ConnectionStatusUpdate {
    SignalingState(String),
    IceConnectionState(String),
    IceGatheringState(String),
}


pub(crate) async fn start_websockets<F: 'static + FnMut(ConnectionStatusUpdate)>(
    delegated_streaming_endpoint: String,
    mut connection_status_callback: F,
) -> anyhow::Result<webrtc::state::WebRtcState> {
    let delegated_streaming_endpoint: Multiaddr = delegated_streaming_endpoint.parse()?;

    let keypair = identity::Keypair::generate_ed25519();
    let org_keypair = identity::Keypair::generate_ed25519();

    let identity = protocols::NodeIdentity::new(keypair, org_keypair)?;
    info!("My public peer id is {}", identity.peer_id);

    let rendezvous_addresses: HashSet<Multiaddr> = HashSet::new();
    // peer_seeds::get_official_rendezvous_ws()
    //     .into_iter()
    //     .collect();

    let configuration = Configuration {
        disable_streaming: true,
        disable_rendezvous_register: true,
        disable_rendezvous_discover: false,
        max_bytes_per_second_upload_stream: None,
        rendezvous_addresses: rendezvous_addresses.clone(),
    };
    let (network_backend_command_sender, mut network_backend_command_receiver) = mpsc::channel(10);

    let networkbackendclient =
        networkbackendclient::NetworkBackendClient::new(network_backend_command_sender);

    let transport = transport::our_transport(identity.keypair.as_ref()).expect("to build protocol");
    let (event_sender, event_receiver) = mpsc::unbounded_channel();
    let (broadcast_event_sender, broadcast_event_receiver) = broadcast::channel(10);
    let (webrtc_signaling_sender, webrtc_signaling_receiver) = mpsc::unbounded_channel();
    let (delegated_streaming_sender, delegated_streaming_receiver) = mpsc::unbounded_channel();

    let (connection_status_update_sender, mut connection_status_update_receiver) =
        mpsc::unbounded_channel();

    let mut swarm = galtcore::swarm::build(
        configuration.clone(),
        identity.clone(),
        event_sender,
        broadcast_event_sender.clone(),
        webrtc_signaling_sender,
        delegated_streaming_sender,
        transport,
    )
    .await;

    for address in rendezvous_addresses {
        match swarm.dial(address.clone()) {
            Ok(_) => log::info!("Dialing rendezvous point address {}", address),
            Err(e) => log::warn!("Failed to dial {}: {}", address, e),
        }
    }

    let shared_global_state = SharedGlobalState::new();

    let gossip_listener_client = GossipListenerClient::new(networkbackendclient.clone());

    galtcore::utils::spawn_and_log_error(daemons::internal_network_events::run_loop(
        event_receiver,
        broadcast_event_receiver,
        gossip_listener_client,
        configuration.clone(),
        identity.clone(),
        shared_global_state,
        networkbackendclient.clone(),
    ));

    match swarm.dial(delegated_streaming_endpoint.clone()) {
        Ok(_) => log::info!("Dialing delegated streaming endpoint {delegated_streaming_endpoint}"),
        Err(e) => log::warn!("Failed to dial {}: {}", delegated_streaming_endpoint, e),
    };

    let webrtc_state = webrtc::state::WebRtcState::new(networkbackendclient.clone());
    galtcore::utils::wspawn_and_log_error({
        let broadcast_event_receiver = broadcast_event_sender.subscribe();
        let webrtc_state = webrtc_state.clone();
        async move {
            webrtc_state
                .run_receive_loop(
                    broadcast_event_receiver,
                    webrtc_signaling_receiver,
                    delegated_streaming_receiver,
                    connection_status_update_sender,
                    delegated_streaming_endpoint,
                )
                .await
        }
    });

    galtcore::utils::wspawn_and_log_error(async move {
        while let Some(event) = connection_status_update_receiver.recv().await {
            connection_status_callback(event);
            tokio::task::yield_now().await;
        }
        log::info!("Exiting from connection_status_update_receiver loop...");
        Ok(())
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
                event = swarm.select_next_some() => protocols::handle_swarm_event(event, &configuration, &identity, &mut swarm)
            };
        }
    });
    Ok(webrtc_state)
}
