// SPDX-License-Identifier: AGPL-3.0-only

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::Parser;
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
    let rtmp_listen_address = cli.rtmp_listen_address;
    backend::start_command(cli).await?;

    gui::run_gui(rtmp_listen_address.to_string());

    Ok(())
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
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
