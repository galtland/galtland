// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use galtcore::configuration::Configuration;
use galtcore::daemons::gossip_listener::GossipListenerClient;
use galtcore::daemons::{self};
use galtcore::libp2p::futures::StreamExt;
use galtcore::libp2p::identity::{self, ed25519};
use galtcore::libp2p::multiaddr::Protocol;
use galtcore::libp2p::{self, Multiaddr};
use galtcore::tokio::sync::{broadcast, mpsc};
use galtcore::{networkbackendclient, protocols, tokio, utils};
use log::{info, warn};

use crate::Cli;


pub(crate) async fn start_command(opt: Cli) -> anyhow::Result<()> {
    let mut db = appcommon::db::Db::get().await?;
    let keypair = match opt.secret_key_seed {
        Some(seed) => {
            let mut bytes: [u8; 32] = [0u8; 32];
            bytes[0] = seed;
            let secret_key = ed25519::SecretKey::from_bytes(&mut bytes).expect(
                "this returns `Err` only if the length is wrong; the length is correct; qed",
            );
            identity::Keypair::Ed25519(secret_key.into())
        }
        None => db.get_or_create_keypair().await?,
    };
    let my_peer_id = keypair.public().to_peer_id();
    info!("My public peer id is {}", my_peer_id);
    let identity = protocols::NodeIdentity {
        keypair: keypair.clone(),
        peer_id: my_peer_id,
    };

    let mut rendezvous_addresses: HashSet<Multiaddr> =
        opt.rendezvous_addresses.into_iter().collect();

    if !opt.disable_official_rendezvous {
        rendezvous_addresses.extend(galtcore::peer_seeds::get_official_rendezvous());
    }

    log::debug!("Rendezvous addresses: {rendezvous_addresses:?}");

    let configuration = Configuration {
        disable_streaming: opt.disable_streaming,
        disable_rendezvous_discover: opt.disable_rendezvous_discover,
        disable_rendezvous_register: opt.disable_rendezvous_register,
        max_bytes_per_second_upload_stream: opt.max_bytes_per_second_upload_stream,
        rendezvous_addresses: rendezvous_addresses.clone(),
    };

    // It's unbounded because we can' block the main loop (but of course we can drop events on the receive side)
    let (event_sender, event_receiver) = mpsc::unbounded_channel();
    let (broadcast_event_sender, broadcast_event_receiver) = broadcast::channel(10);
    let (webrtc_signaling_sender, _webrtc_signaling_receiver) = mpsc::unbounded_channel();
    let (delegated_streaming_sender, _delegated_streaming_receiver) = mpsc::unbounded_channel();
    let transport = nativecommon::transport::our_transport(&keypair).await?;
    let mut swarm: libp2p::Swarm<protocols::ComposedBehaviour> = galtcore::swarm::build(
        configuration.clone(),
        keypair,
        event_sender,
        broadcast_event_sender,
        webrtc_signaling_sender,
        delegated_streaming_sender,
        transport,
    )
    .await;

    let (network_backend_command_sender, mut network_backend_command_receiver) = mpsc::channel(10);

    let (highlevel_command_sender, highlevel_command_receiver) = mpsc::channel(10);

    let networkbackendclient =
        networkbackendclient::NetworkBackendClient::new(network_backend_command_sender);

    if !opt.no_listen {
        let mut listen_addresses = opt.listen_addresses.clone();
        if listen_addresses.is_empty() {
            listen_addresses.push(
                Multiaddr::empty()
                    .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
                    .with(Protocol::Tcp(0)),
            );
            listen_addresses.push(
                Multiaddr::empty()
                    .with(Protocol::from(Ipv6Addr::UNSPECIFIED))
                    .with(Protocol::Tcp(0)),
            );
        }
        for listen_address in listen_addresses {
            if let Err(e) = swarm.listen_on(listen_address) {
                log::warn!("Error listening: {e:?}");
            };
        }
    }

    // utils::spawn_and_log_error(rtmp_server::accept_loop(
    //     opt.rtmp_listen_address,
    //     identity.clone(),
    //     highlevel_command_sender.clone(),
    // ));

    for address in rendezvous_addresses {
        match swarm.dial(address.clone()) {
            Ok(_) => info!("Dialing rendezvous point address {}", address),
            Err(e) => warn!("Failed to dial {}: {}", address, e),
        }
    }

    if let Some(address) = &opt.dial_address {
        match swarm.dial(address.clone()) {
            Ok(_) => info!("Dialing address {}", address),
            Err(e) => warn!("Failed to dial {}: {}", address, e),
        }
    }

    utils::spawn_and_log_error(daemons::cm::run_loop(
        configuration.clone(),
        networkbackendclient.clone(),
        highlevel_command_receiver,
        identity,
    ));

    let gossip_listener_client = GossipListenerClient::new(networkbackendclient);

    utils::spawn_and_log_error(daemons::internal_network_events::run_loop(
        event_receiver,
        broadcast_event_receiver,
        highlevel_command_sender,
        gossip_listener_client,
    ));

    utils::spawn_and_log_error({
        let mut discover_tick = tokio::time::interval(Duration::from_secs(30));
        async move {
            loop {
                log::trace!("Looping main loop");
                tokio::task::yield_now().await;
                tokio::select! {
                    _ = discover_tick.tick() => {
                        protocols::handle_discover_tick(&configuration, &mut swarm)
                    },
                    command = network_backend_command_receiver.recv() => match command {
                        Some(e) => protocols::handle_network_backend_command(e, &mut swarm)?,
                        None => todo!(),
                    },
                    event = swarm.select_next_some() => protocols::handle_swarm_event(event, &configuration, &mut swarm)
                };
            }
        }
    });
    Ok(())
}
