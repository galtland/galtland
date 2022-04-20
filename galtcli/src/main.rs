// SPDX-License-Identifier: AGPL-3.0-only

use std::borrow::Cow;
use std::collections::HashSet;
use std::io::Write;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use galtcore::configuration::Configuration;
use galtcore::daemons::gossip_listener::GossipListenerClient;
use galtcore::libp2p::futures::StreamExt;
use galtcore::libp2p::identity::{self, ed25519};
use galtcore::libp2p::multiaddr::Protocol;
use galtcore::libp2p::{self, Multiaddr, PeerId};
use galtcore::protocols::media_streaming::StreamingKey;
use galtcore::tokio::sync::{broadcast, mpsc};
use galtcore::{daemons, networkbackendclient, protocols, tokio, utils};
use log::{info, warn};
use rand::Rng;
use service::sm;
use tonic::transport::Uri;

pub mod service;

const DEFAULT_API_ADDRESS: &str = "127.0.0.1:51234";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    human_panic::setup_panic!();
    pretty_env_logger::init_timed();
    let cli = Cli::parse();
    let api_listen_address = cli.api_listen_address;
    match cli.command {
        MainCommands::Start(opt) => start_command(api_listen_address, opt).await,
        MainCommands::Files(opt) => files_command(api_listen_address, opt).await,
        MainCommands::Network(opt) => network_command(api_listen_address, opt).await,
        MainCommands::Stream(opt) => stream_command(api_listen_address, opt).await,
    }
}

async fn start_command(
    api_listen_address: Option<std::net::SocketAddr>,
    opt: StartOpt,
) -> anyhow::Result<()> {
    let mut db = appcommon::db::Db::get().await?;
    let keypair = match opt.secret_key_seed {
        Some(seed) => {
            let mut bytes: [u8; 32] = [0u8; 32];
            bytes[0] = seed;
            let secret_key = ed25519::SecretKey::from_bytes(&mut bytes).context(
                "this returns `Err` only if the length is wrong; the length is correct; qed",
            )?;
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
    let (webrtc_signaling_sender, webrtc_signaling_receiver) = mpsc::unbounded_channel();
    let (delegated_streaming_sender, delegated_streaming_receiver) = mpsc::unbounded_channel();
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

    utils::spawn_and_log_error(nativecommon::webrtc::WebRtc::webrtc_main_loop(
        webrtc_signaling_receiver,
        delegated_streaming_receiver,
        networkbackendclient.clone(),
        highlevel_command_sender.clone(),
        identity.clone(),
    ));

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
            listen_addresses.push(
                Multiaddr::empty()
                    .with(Protocol::from(Ipv4Addr::LOCALHOST))
                    .with(Protocol::Tcp(8085))
                    .with(Protocol::Ws(Cow::Borrowed("/"))),
            );
            listen_addresses.push(
                Multiaddr::empty()
                    .with(Protocol::from(Ipv4Addr::UNSPECIFIED))
                    .with(Protocol::Tcp(8086))
                    .with(Protocol::Wss(Cow::Borrowed("/"))),
            );
        }
        for listen_address in listen_addresses {
            if let Err(e) = swarm.listen_on(listen_address) {
                log::warn!("Error listening: {e:?}");
            };
        }
    }

    utils::spawn_and_log_error(nativecommon::rtmp_server2::accept_loop(
        opt.rtmp_listen_address
            .unwrap_or_else(|| "127.0.0.1:1935".parse().unwrap()),
        identity.clone(),
        highlevel_command_sender.clone(),
    ));

    if !opt.disable_rendezvous_register && !opt.disable_rendezvous_discover {
        for address in rendezvous_addresses {
            match swarm.dial(address.clone()) {
                Ok(_) => info!("Dialing rendezvous point address {}", address),
                Err(e) => warn!("Failed to dial {}: {}", address, e),
            }
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
        identity.clone(),
    ));

    let gossip_listener_client = GossipListenerClient::new(networkbackendclient.clone());

    utils::spawn_and_log_error({
        let api_address = api_listen_address.unwrap_or(DEFAULT_API_ADDRESS.parse()?);
        let service = service::Service {
            commands: highlevel_command_sender.clone(),
            network: networkbackendclient.clone(),
            gossip_listener_client: gossip_listener_client.clone(),
            identity,
        };
        log::info!("Starting service api on {api_address}");
        async move {
            tonic::transport::Server::builder()
                .add_service(service::sm::service_server::ServiceServer::new(service))
                .serve(api_address)
                .await
                .map_err(|e| anyhow::anyhow!("Service error: {e}"))
        }
    });

    utils::spawn_and_log_error(daemons::internal_network_events::run_loop(
        event_receiver,
        broadcast_event_receiver,
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
                Some(e) => protocols::handle_network_backend_command(e, &mut swarm)?,
                None => todo!(),
            },
            event = swarm.select_next_some() => protocols::handle_swarm_event(event, &configuration, &mut swarm)
        };
    }
}

async fn get_service_client(
    api_listen_address: Option<std::net::SocketAddr>,
) -> anyhow::Result<service::sm::service_client::ServiceClient<tonic::transport::Channel>> {
    let dst = api_listen_address.unwrap_or(DEFAULT_API_ADDRESS.parse()?);
    let dst = Uri::builder()
        .scheme("http")
        .authority(dst.to_string())
        .path_and_query("")
        .build()?;
    service::sm::service_client::ServiceClient::connect(dst)
        .await
        .context("connecting to ")
}

async fn files_command(
    api_listen_address: Option<std::net::SocketAddr>,
    files_opt: FilesOpt,
) -> anyhow::Result<()> {
    let mut client = get_service_client(api_listen_address).await?;
    match files_opt.command {
        FilesCommands::Add(add_opt) => {
            let request = tonic::Request::new(service::sm::AddSimpleFileRequest {
                filename: add_opt.filename,
            });
            let response = client.add_simple_file(request).await?;
            let response = response.into_inner();
            println!("{response:?}")
        }
        FilesCommands::Get(get_opt) => {
            let request = tonic::Request::new(service::sm::GetSimpleFileRequest {
                hash_hex: get_opt.hash_hex,
            });
            let response = client.get_simple_file(request).await?;
            let mut response = response.into_inner();
            while let Some(r) = response.next().await {
                match r {
                    Ok(r) => {
                        std::io::stdout()
                            .lock()
                            .write_all(&r.data)
                            .context("failed to write")?;
                    }
                    Err(e) => {
                        eprintln!("Application error: {}", e);
                        std::process::exit(1);
                    }
                }
                tokio::task::yield_now().await;
            }
        }
    }
    Ok(())
}

async fn network_command(
    api_listen_address: Option<std::net::SocketAddr>,
    network_opt: NetworkOpt,
) -> anyhow::Result<()> {
    let mut client = get_service_client(api_listen_address).await?;
    match network_opt.command {
        NetworkCommands::Info => {
            match client.get_network_info(sm::GetNetworkInfoRequest {}).await {
                Ok(r) => {
                    let r = r.into_inner();
                    let peer_id = r.peer_id;
                    let info = r.info;
                    println!("Peer id: {peer_id:?}\n\n{info}");
                }
                Err(e) => {
                    eprintln!("{}", e)
                }
            }
        }
    }
    Ok(())
}

async fn stream_command(
    api_listen_address: Option<std::net::SocketAddr>,
    opt: StreamOpt,
) -> anyhow::Result<()> {
    let mut client = get_service_client(api_listen_address).await?;
    match opt.command {
        StreamCommands::PublishFile(args) => {
            let peer_id = match client.get_network_info(sm::GetNetworkInfoRequest {}).await {
                Ok(r) => r.into_inner().peer_id,
                Err(e) => {
                    anyhow::bail!("error getting network info: {e}")
                }
            };
            let video_stream_key = rand::thread_rng().gen::<[u8; 32]>();
            let streaming_key = StreamingKey {
                video_key: video_stream_key.into(),
                channel_key: PeerId::from_bytes(&peer_id)?,
            };
            println!("Publishing to stream key:\n{streaming_key}");
            match client
                .stream_publish_file(sm::StreamPublishFileRequest {
                    video_file: args.video_file,
                    audio_file: args.audio_file,
                    video_stream_key: video_stream_key.into(),
                })
                .await
            {
                Ok(r) => println!("Result: {}", r.into_inner().message),
                Err(e) => {
                    eprintln!("Application error: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
    Ok(())
}


#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long)]
    api_listen_address: Option<std::net::SocketAddr>,

    #[clap(subcommand)]
    command: MainCommands,
}

#[derive(clap::Args)]
struct StartOpt {
    #[clap(long)]
    secret_key_seed: Option<u8>,

    #[clap(long)]
    listen_addresses: Vec<Multiaddr>,

    #[clap(long)]
    no_listen: bool,

    #[clap(long)]
    disable_streaming: bool,

    #[clap(long)]
    disable_rendezvous_register: bool,

    #[clap(long)]
    disable_official_rendezvous: bool,

    #[clap(long)]
    disable_rendezvous_discover: bool,

    #[clap(long)]
    rendezvous_addresses: Vec<Multiaddr>,

    #[clap(long)]
    dial_address: Option<Multiaddr>,

    #[clap(long)]
    rtmp_listen_address: Option<std::net::SocketAddr>,

    #[clap(short, long)]
    max_bytes_per_second_upload_stream: Option<byte_unit::Byte>,
}

#[derive(clap::Args)]
struct FilesOpt {
    #[clap(subcommand)]
    command: FilesCommands,
}

#[derive(Subcommand)]
enum MainCommands {
    Start(StartOpt),
    Files(FilesOpt),
    Network(NetworkOpt),
    Stream(StreamOpt),
}

#[derive(clap::Args)]
struct FilesAddOpt {
    filename: String,
}

#[derive(clap::Args)]
struct FilesGetOpt {
    hash_hex: String,
}

#[derive(Subcommand)]
enum FilesCommands {
    Add(FilesAddOpt),
    Get(FilesGetOpt),
}

#[derive(clap::Args)]
struct NetworkOpt {
    #[clap(subcommand)]
    command: NetworkCommands,
}

#[derive(Subcommand)]
enum NetworkCommands {
    Info,
}


#[derive(clap::Args)]
struct StreamOpt {
    #[clap(subcommand)]
    command: StreamCommands,
}

#[derive(Subcommand)]
enum StreamCommands {
    PublishFile(StreamPublishFileArgs),
}

#[derive(clap::Args)]
struct StreamPublishFileArgs {
    video_file: String,
    audio_file: String,
}
