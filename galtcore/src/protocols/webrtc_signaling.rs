// SPDX-License-Identifier: AGPL-3.0-only

use async_trait::async_trait;
use futures::{io, AsyncRead, AsyncWrite};
use libp2p::core::upgrade::{read_length_prefixed, read_varint};
use libp2p::core::ProtocolName;
use libp2p::request_response::{
    RequestResponseCodec, RequestResponseEvent, RequestResponseMessage, ResponseChannel,
};
use libp2p::{PeerId, Swarm};
use log::warn;

use super::ComposedBehaviour;
use crate::utils;

#[derive(Clone)]
pub struct WebRtcSignalingProtocol();
#[derive(Clone)]
pub struct WebRtcSignalingCodec();

#[derive(Debug)]
pub struct SignalingRequest {
    pub offer: Option<Vec<u8>>,
    pub ice_candidates: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Default)]
pub struct SignalingResponse {
    pub answer: Option<Vec<u8>>,
    pub ice_candidates: Vec<Option<Vec<u8>>>,
}

pub struct RequestEvent {
    pub peer: PeerId,
    pub request: SignalingRequest,
    pub channel: ResponseChannel<WebRtcSignalingResponseResult>,
}

pub type WebRtcSignalingResponseResult = Result<SignalingResponse, String>;

impl ProtocolName for WebRtcSignalingProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/webrtc-signaling/1".as_bytes()
    }
}

const MAX_SIZE: usize = 100_000;

#[async_trait]
impl RequestResponseCodec for WebRtcSignalingCodec {
    type Protocol = WebRtcSignalingProtocol;
    type Request = SignalingRequest;
    type Response = WebRtcSignalingResponseResult;

    async fn read_request<T>(
        &mut self,
        _: &WebRtcSignalingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let offer = read_length_prefixed(io, MAX_SIZE).await?;
        let offer = if offer.is_empty() { None } else { Some(offer) };
        let len = read_varint(io).await?;
        let mut ice_candidates = Vec::with_capacity(len);
        for _ in 0..len {
            let i = read_length_prefixed(io, MAX_SIZE).await?;
            let i = if i.is_empty() { None } else { Some(i) };
            ice_candidates.push(i);
        }
        Ok(SignalingRequest {
            offer,
            ice_candidates,
        })
    }

    async fn read_response<T>(
        &mut self,
        _: &WebRtcSignalingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        match read_varint(io).await? {
            1 => {
                let s = read_length_prefixed(io, MAX_SIZE).await?;
                let s = std::str::from_utf8(&s).map_err(crate::utils::utf8_error)?;
                Ok(Err(s.to_string()))
            }
            2 => {
                let answer = read_length_prefixed(io, MAX_SIZE).await?;
                let answer = if answer.is_empty() {
                    None
                } else {
                    Some(answer)
                };
                let len = read_varint(io).await?;
                let mut ice_candidates = Vec::with_capacity(len);
                for _ in 0..len {
                    let i = read_length_prefixed(io, MAX_SIZE).await?;
                    let i = if i.is_empty() { None } else { Some(i) };
                    ice_candidates.push(i);
                }
                Ok(Ok(SignalingResponse {
                    answer,
                    ice_candidates,
                }))
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Error reading response: {other} is a invalid code"),
            )),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &WebRtcSignalingProtocol,
        io: &mut T,
        r: SignalingRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        utils::write_limited_length_prefixed(io, r.offer.unwrap_or_default(), MAX_SIZE).await?;
        utils::write_varint(io, r.ice_candidates.len()).await?;
        for i in &r.ice_candidates {
            let i = match i {
                Some(i) => i.as_slice(),
                None => &[],
            };
            utils::write_limited_length_prefixed(io, i, MAX_SIZE).await?;
        }
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &WebRtcSignalingProtocol,
        io: &mut T,
        r: WebRtcSignalingResponseResult,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match r {
            Ok(r) => {
                utils::write_varint(io, 2).await?;
                utils::write_limited_length_prefixed(io, r.answer.unwrap_or_default(), MAX_SIZE)
                    .await?;
                utils::write_varint(io, r.ice_candidates.len()).await?;
                for i in &r.ice_candidates {
                    let i = match i {
                        Some(i) => i.as_slice(),
                        None => &[],
                    };
                    utils::write_limited_length_prefixed(io, i, MAX_SIZE).await?;
                }
            }
            Err(e) => {
                utils::write_varint(io, 1).await?;
                utils::write_limited_length_prefixed(io, e.as_bytes(), MAX_SIZE).await?;
            }
        };
        Ok(())
    }
}

pub(crate) fn handle_event(
    event: RequestResponseEvent<SignalingRequest, WebRtcSignalingResponseResult>,
    swarm: &mut Swarm<ComposedBehaviour>,
) {
    match event {
        RequestResponseEvent::Message { peer, message } => match message {
            RequestResponseMessage::Request {
                request_id,
                request,
                channel,
            } => {
                let offer_len = match &request.offer {
                    Some(offer) => offer.len(),
                    None => 0,
                };

                let ice_candidates_len: usize = request
                    .ice_candidates
                    .iter()
                    .map(|i| match i {
                        Some(i) => i.len(),
                        None => 0,
                    })
                    .sum();

                log::debug!(
                    "RequestResponseMessage::Request {} {} {} offer bytes {} ice candidates bytes",
                    request_id,
                    peer,
                    offer_len,
                    ice_candidates_len
                );
                if let Err(e) = swarm
                    .behaviour_mut()
                    .webrtc_signaling_sender
                    .send(RequestEvent {
                        peer,
                        request,
                        channel,
                    })
                {
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
                    .pending_webrtc_signaling_request
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
            if let Err(e) = swarm
                .behaviour_mut()
                .state
                .pending_webrtc_signaling_request
                .remove(&request_id)
                .expect("Request to still be pending")
                .send(Err(error))
            {
                warn!("Receiver dropped while trying to send: {e:?}")
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
