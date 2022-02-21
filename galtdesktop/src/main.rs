use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::{Parser, Subcommand};
use galtcore::libp2p::futures::StreamExt;
use galtcore::libp2p::Multiaddr;
use galtcore::tokio;


pub mod backend;
pub mod gui;
pub mod video;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    human_panic::setup_panic!();
    pretty_env_logger::init_timed();
    let cli = Cli::parse();
    let (h, rtmp_listen_address) = match cli.command {
        MainCommands::Start(opt) => {
            let rtmp_listen_address = opt.rtmp_listen_address;
            let h = galtcore::utils::spawn_and_log_error(backend::start_command(opt));
            (h, rtmp_listen_address)
        }
    };

    gui::run_gui(format!("rtmp://{}/", rtmp_listen_address));

    Ok(h.await?)
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: MainCommands,
}

#[derive(clap::Args)]
pub struct StartOpt {
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

    #[clap(long,default_value_t = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1935)))]
    rtmp_listen_address: SocketAddr,

    #[clap(short, long)]
    max_bytes_per_second_upload_stream: Option<byte_unit::Byte>,
}


#[derive(Subcommand)]
enum MainCommands {
    Start(StartOpt),
}
