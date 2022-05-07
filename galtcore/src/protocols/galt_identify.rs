// SPDX-License-Identifier: AGPL-3.0-only


use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{io, AsyncRead, AsyncWrite};
use libp2p::core::upgrade::{read_length_prefixed, read_varint};
use libp2p::core::{ProtocolName, PublicKey};
use libp2p::request_response::{
    RequestResponseCodec, RequestResponseEvent, RequestResponseMessage, ResponseChannel,
};
use libp2p::{multiaddr, Multiaddr, PeerId, Swarm};
use log::warn;

use super::ComposedBehaviour;
use crate::configuration::Configuration;
use crate::daemons::internal_network_events::InternalNetworkEvent;
use crate::utils;

#[derive(Clone)]
pub struct GaltIdentifyProtocol();
#[derive(Clone)]
pub struct GaltIdentifyCodec();

pub const MAX_SHARED_PEERS_PER_LIST: usize = 100;
pub const MAX_SHARED_ADDRESSES_PER_PEER: usize = 100;

#[derive(Clone, Eq)]
pub struct GaltOrganization {
    public_key: Arc<libp2p::core::PublicKey>,
    public_key_bytes: Bytes,
}

impl Debug for GaltOrganization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GaltOrganization")
            .field("name", &bs58::encode(&self.public_key_bytes).into_string())
            .finish()
    }
}

impl PartialEq for GaltOrganization {
    fn eq(&self, other: &Self) -> bool {
        self.public_key == other.public_key
    }
}

impl std::hash::Hash for GaltOrganization {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.public_key_bytes.hash(state);
    }
}

impl GaltOrganization {
    pub fn new(public_key: libp2p::core::PublicKey) -> Self {
        let public_key_bytes = public_key.to_protobuf_encoding();
        Self::new_private(public_key, public_key_bytes)
    }

    fn new_private(public_key: libp2p::core::PublicKey, public_key_bytes: Vec<u8>) -> Self {
        Self {
            public_key: Arc::new(public_key),
            public_key_bytes: public_key_bytes.into(),
        }
    }
}

pub type KnownPeerAddresses = Vec<Multiaddr>;

#[derive(Clone)]
pub struct KnownPeer {
    pub peer: PeerId,
    pub external_addresses: KnownPeerAddresses,
}

impl KnownPeer {
    pub fn canonicalize(self) -> Self {
        let external_addresses =
            utils::canonicalize_addresses(self.external_addresses.into_iter(), &self.peer);
        Self {
            peer: self.peer,
            external_addresses,
        }
    }
}


pub struct GaltIdentifyInfo {
    pub external_addresses: KnownPeerAddresses,
    pub our_org_peers: Vec<KnownPeer>,
    pub org: GaltOrganization,
    pub our_org_sig: Vec<u8>,
    pub other_peers: Vec<KnownPeer>,
}

impl GaltIdentifyInfo {
    pub(crate) fn verify_sig(&self, peer: &PeerId) -> bool {
        self.org
            .public_key
            .verify(&peer.to_bytes(), &self.our_org_sig)
    }
}

pub enum GaltIdentifyRequest {
    OfferInfo(GaltIdentifyInfo),
}


pub struct GaltIdentifyResponse {}


pub struct RequestEvent {
    pub peer: PeerId,
    pub request: GaltIdentifyRequest,
    pub channel: ResponseChannel<GaltIdentifyResponseResult>,
}

pub type GaltIdentifyResponseResult = Result<GaltIdentifyResponse, String>;

impl ProtocolName for GaltIdentifyProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/galt-identify/1".as_bytes()
    }
}

const MAX_SIZE: usize = 10_000;

async fn deserialize_external_addresses<T: AsyncRead + Unpin + Send>(
    io: &mut T,
) -> io::Result<KnownPeerAddresses> {
    let external_addresses_len = read_varint(io).await?;
    if external_addresses_len > MAX_SHARED_ADDRESSES_PER_PEER {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("too many addresses for peer: {external_addresses_len}"),
        ));
    }
    let mut external_addresses = Vec::with_capacity(external_addresses_len);
    for _ in 0..external_addresses_len {
        let multiaddr = read_length_prefixed(io, MAX_SIZE).await?;
        let multiaddr: Multiaddr = multiaddr.try_into().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid multiaddr {e:?}"),
            )
        })?;
        external_addresses.push(multiaddr);
    }
    Ok(external_addresses)
}

async fn deserialize_known_peer<T: AsyncRead + Unpin + Send>(io: &mut T) -> io::Result<KnownPeer> {
    let peer = read_length_prefixed(io, MAX_SIZE).await?;
    let peer: PeerId = PeerId::from_bytes(&peer).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid peer id: {e:?}"),
        )
    })?;
    let external_addresses = deserialize_external_addresses(io).await?;
    Ok(KnownPeer {
        peer,
        external_addresses,
    })
}


async fn serialize_external_addresses<T: AsyncWrite + Unpin + Send>(
    io: &mut T,
    mut external_addresses: &[Multiaddr],
) -> io::Result<usize> {
    let mut n = 0;

    if external_addresses.len() > MAX_SHARED_ADDRESSES_PER_PEER {
        log::debug!(
            "truncating address list from {} to {}",
            external_addresses.len(),
            MAX_SHARED_ADDRESSES_PER_PEER
        );
        external_addresses = &external_addresses[..MAX_SHARED_ADDRESSES_PER_PEER];
    }
    n += utils::write_varint(io, external_addresses.len()).await?;
    for external_address in external_addresses {
        n += utils::write_limited_length_prefixed(io, external_address, MAX_SIZE).await?;
    }

    Ok(n)
}

async fn serialize_known_peers<T: AsyncWrite + Unpin + Send>(
    io: &mut T,
    peers: &[KnownPeer],
) -> io::Result<usize> {
    let mut n = 0;
    let peers_len = peers.len();
    if peers_len > MAX_SHARED_PEERS_PER_LIST {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("too many peers on list: {peers_len}"),
        ));
    }
    n += utils::write_varint(io, peers_len).await?;
    for k in peers {
        n += utils::write_limited_length_prefixed(io, &k.peer.to_bytes(), MAX_SIZE).await?;
        n += serialize_external_addresses(io, &k.external_addresses).await?;
    }
    Ok(n)
}


async fn deserialize_known_peers<T: AsyncRead + Unpin + Send>(
    io: &mut T,
) -> io::Result<Vec<KnownPeer>> {
    let peers_len = read_varint(io).await?;
    if peers_len > MAX_SHARED_PEERS_PER_LIST {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("too many peers on list: {peers_len}"),
        ));
    }
    let mut peers = Vec::with_capacity(peers_len);
    for _ in 0..peers_len {
        let known_peer = deserialize_known_peer(io).await?;
        peers.push(known_peer);
    }
    Ok(peers)
}


async fn deserialize_info<T: AsyncRead + Unpin + Send>(io: &mut T) -> io::Result<GaltIdentifyInfo> {
    let org_public_key_bytes = read_length_prefixed(io, MAX_SIZE).await?;
    let org_public_key = PublicKey::from_protobuf_encoding(&org_public_key_bytes).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid org public key: {e:?}"),
        )
    })?;
    let org = GaltOrganization::new_private(org_public_key, org_public_key_bytes);
    let our_org_sig = read_length_prefixed(io, MAX_SIZE).await?;
    let external_addresses = deserialize_external_addresses(io).await?;
    let our_org_peers = deserialize_known_peers(io).await?;
    let other_peers = deserialize_known_peers(io).await?;

    Ok(GaltIdentifyInfo {
        external_addresses,
        our_org_peers,
        org,
        our_org_sig,
        other_peers,
    })
}


async fn serialize_info<T: AsyncWrite + Unpin + Send>(
    io: &mut T,
    info: &GaltIdentifyInfo,
) -> io::Result<usize> {
    let mut n = 0;
    n += utils::write_limited_length_prefixed(io, &info.org.public_key_bytes, MAX_SIZE).await?;
    n += utils::write_limited_length_prefixed(io, &info.our_org_sig, MAX_SIZE).await?;
    n += serialize_external_addresses(io, &info.external_addresses).await?;
    n += serialize_known_peers(io, &info.our_org_peers).await?;
    n += serialize_known_peers(io, &info.other_peers).await?;
    Ok(n)
}


#[async_trait]
impl RequestResponseCodec for GaltIdentifyCodec {
    type Protocol = GaltIdentifyProtocol;
    type Request = GaltIdentifyRequest;
    type Response = GaltIdentifyResponseResult;

    async fn read_request<T>(
        &mut self,
        _: &GaltIdentifyProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let i = read_varint(io).await?;
        match i {
            1 => {
                let info = deserialize_info(io).await?;
                Ok(GaltIdentifyRequest::OfferInfo(info))
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Error reading GaltIdentifyRequest: {other} is a invalid code"),
            )),
        }
    }

    async fn read_response<T>(
        &mut self,
        _: &GaltIdentifyProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        match read_varint(io).await? {
            1 => {
                let s = utils::read_to_string(io, MAX_SIZE, "GaltIdentifyProtocol error string")
                    .await?;
                Ok(Err(s))
            }
            2 => Ok(Ok(GaltIdentifyResponse {})),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Error reading GaltIdentifyResponse: {other} is a invalid code"),
            )),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &GaltIdentifyProtocol,
        io: &mut T,
        r: GaltIdentifyRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match &r {
            GaltIdentifyRequest::OfferInfo(info) => {
                utils::write_varint(io, 1).await?;
                serialize_info(io, info).await?;
            }
        };
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &GaltIdentifyProtocol,
        io: &mut T,
        r: GaltIdentifyResponseResult,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match r {
            Err(e) => {
                utils::write_varint(io, 1).await?;
                utils::write_limited_length_prefixed(io, e.as_bytes(), MAX_SIZE).await?;
            }
            Ok(GaltIdentifyResponse {}) => {
                utils::write_varint(io, 2).await?;
            }
        };
        Ok(())
    }
}


pub(crate) fn handle_event(
    event: RequestResponseEvent<GaltIdentifyRequest, GaltIdentifyResponseResult>,
    opt: &Configuration,
    swarm: &mut Swarm<ComposedBehaviour>,
) {
    match event {
        RequestResponseEvent::Message { peer, message } => match message {
            RequestResponseMessage::Request {
                request_id,
                request,
                channel,
            } => {
                log::debug!("RequestResponseMessage::Request {} {}", request_id, peer,);
                if let Some(state) = swarm.behaviour_mut().state.rendezvous_peers.get_mut(&peer) {
                    let addresses = match &request {
                        GaltIdentifyRequest::OfferInfo(i) => utils::canonicalize_addresses(
                            i.external_addresses.iter().cloned(),
                            &peer,
                        ),
                    };
                    log::info!(
                        "Saving {} addresses from rendezvous {peer}",
                        addresses.len()
                    );
                    state.addresses = addresses.clone();
                    if !opt.disable_rendezvous_relay {
                        for addr in addresses {
                            match utils::canonical_address_for_peer_id(
                                addr.clone().with(multiaddr::Protocol::P2pCircuit),
                                swarm.local_peer_id(),
                            ) {
                                Ok(relay_url) => {
                                    log::info!(
                                        "Trying to listen on relay {peer} address {addr:?} using {relay_url}"
                                    );
                                    if let Err(e) = swarm.listen_on(relay_url) {
                                        log::info!(
                                            "Error listening on relay {peer} address {addr:?}: {e:?}"
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::warn!("Failed to create relay url {addr}: {e:?}")
                                }
                            }
                        }
                    }
                }
                if let Err(e) = swarm.behaviour_mut().event_sender.send(
                    InternalNetworkEvent::GaltIdentifyRequest {
                        peer,
                        request,
                        channel,
                    },
                ) {
                    log::warn!("Error sending event: {e}")
                }
            }
            RequestResponseMessage::Response {
                request_id,
                response,
            } => {
                log::debug!("RequestResponseMessage::Response {} {}", request_id, peer);
                if swarm
                    .behaviour_mut()
                    .state
                    .pending_galt_identify_request
                    .remove(&request_id)
                    .expect("Request to still be pending")
                    .send(Ok(response))
                    .is_err()
                {
                    warn!("Unexpected drop of receiver")
                }
            }
        },
        RequestResponseEvent::OutboundFailure {
            peer,
            request_id,
            error,
        } => {
            warn!(
                "RequestResponseEvent::OutboundFailure {} {}: {}",
                peer, request_id, error
            );
            if swarm
                .behaviour_mut()
                .state
                .pending_galt_identify_request
                .remove(&request_id)
                .expect("Request to still be pending")
                .send(Err(error))
                .is_err()
            {
                warn!("Receiver dropped while trying to send galt identify error")
            };
        }
        RequestResponseEvent::InboundFailure {
            peer,
            request_id,
            error,
        } => warn!(
            "RequestResponseEvent::InboundFailure {} {}: {}",
            peer, request_id, error
        ),
        RequestResponseEvent::ResponseSent { peer, request_id } => log::debug!(
            "RequestResponseEvent::ResponseSent {}: {}",
            peer,
            request_id
        ),
    }
}
