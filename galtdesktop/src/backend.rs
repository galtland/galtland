use std::collections::HashSet;
use std::time::Duration;

use clap::Parser;
use galtcore::configuration::Configuration;
use galtcore::daemons::gossip_listener::GossipListenerClient;
use galtcore::daemons::{self, rtmp_server};
use galtcore::libp2p::futures::StreamExt;
use galtcore::libp2p::identity::{self, ed25519};
use galtcore::libp2p::{self, Multiaddr};
use galtcore::tokio::sync::mpsc;
use galtcore::{networkbackendclient, protocols, tokio, utils};
use log::{info, warn};

use crate::StartOpt;


pub async fn start_command(opt: StartOpt) -> anyhow::Result<()> {
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
    let mut swarm: libp2p::Swarm<protocols::ComposedBehaviour> =
        galtcore::swarm::build(configuration.clone(), keypair, event_sender).await;

    let (network_backend_command_sender, mut network_backend_command_receiver) = mpsc::channel(10);

    let (highlevel_command_sender, highlevel_command_receiver) = mpsc::channel(10);

    let networkbackendclient =
        networkbackendclient::NetworkBackendClient::new(network_backend_command_sender);

    if !opt.no_listen {
        let mut listen_addresses = opt.listen_addresses.clone();
        if listen_addresses.is_empty() {
            listen_addresses.push("/ip4/0.0.0.0/tcp/0".parse().unwrap());
            listen_addresses.push("/ip6/::/tcp/0".parse().unwrap());
        }
        for listen_address in listen_addresses {
            if let Err(e) = swarm.listen_on(listen_address) {
                log::warn!("Error listening: {e:?}");
            };
        }
    }

    utils::spawn_and_log_error(rtmp_server::accept_loop(
        opt.rtmp_listen_address,
        identity.clone(),
        highlevel_command_sender.clone(),
    ));

    for address in rendezvous_addresses {
        match swarm.dial(address.clone()) {
            Ok(_) => info!("Dialing rendezvous point address {}", address),
            Err(e) => warn!("Failed to dial {}: {}", address, e),
        }
    }

    if let Some(address) = opt.dial_address.clone() {
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

    let gossip_listener_client = GossipListenerClient::new(networkbackendclient.clone());

    utils::spawn_and_log_error(daemons::internal_network_events::run_loop(
        event_receiver,
        highlevel_command_sender.clone(),
        gossip_listener_client.clone(),
    ));

    let mut discover_tick = tokio::time::interval(Duration::from_secs(30));
    loop {
        log::trace!("Looping main loop");
        tokio::task::yield_now().await;
        tokio::select! {
            _ = discover_tick.tick() => {
                 protocols::handle_discover_tick(&configuration, &mut swarm)
            },
            command = network_backend_command_receiver.recv() => match command {
                Some(e) => protocols::handle_network_backend_command(e, &mut swarm),
                None => todo!(),
            },
            event = swarm.select_next_some() => protocols::handle_swarm_event(event, &configuration, &mut swarm)
        };
    }
}
