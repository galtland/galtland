use async_trait::async_trait;
use libp2p::core::upgrade::{
    read_length_prefixed, read_varint, write_length_prefixed, write_varint,
};
use libp2p::core::ProtocolName;
use libp2p::futures::{io, AsyncRead, AsyncWrite};
use libp2p::request_response::{
    RequestResponseCodec, RequestResponseEvent, RequestResponseMessage,
};
use libp2p::Swarm;
use log::warn;

use super::ComposedBehaviour;
use crate::protocols::InternalNetworkEvent;
use crate::utils;

// Simple file exchange protocol
#[derive(Debug, Clone)]
pub struct SimpleFileExchangeProtocol();
#[derive(Clone)]
pub struct SimpleFileExchangeCodec();
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleFileRequest {
    pub key: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleFileResponse {
    pub eof: bool,
    pub data: Vec<u8>,
}

impl ProtocolName for SimpleFileExchangeProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/simple-file-exchange/1".as_bytes()
    }
}

#[async_trait]
impl RequestResponseCodec for SimpleFileExchangeCodec {
    type Protocol = SimpleFileExchangeProtocol;
    type Request = SimpleFileRequest;
    type Response = Result<SimpleFileResponse, String>;

    async fn read_request<T>(
        &mut self,
        _: &SimpleFileExchangeProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let key = read_length_prefixed(io, 10_000_000).await?;

        Ok(SimpleFileRequest { key })
    }

    async fn read_response<T>(
        &mut self,
        _: &SimpleFileExchangeProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        match read_varint(io).await? {
            usize::MAX => {
                let data = read_length_prefixed(io, 10_000).await?;
                match std::str::from_utf8(&data) {
                    Ok(s) => Ok(Err(s.into())),
                    Err(e) => Err(crate::utils::utf8_error(e)),
                }
            }
            flags => {
                let eof = flags == 1;
                let data = read_length_prefixed(io, 10_000_000).await?;
                Ok(Ok(SimpleFileResponse { eof, data }))
            }
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &SimpleFileExchangeProtocol,
        io: &mut T,
        r: SimpleFileRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_length_prefixed(io, r.key).await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &SimpleFileExchangeProtocol,
        io: &mut T,
        r: Result<SimpleFileResponse, String>,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match r {
            Ok(r) => {
                let flags = if r.eof { 1 } else { 0 };
                write_varint(io, flags).await?;
                write_length_prefixed(io, r.data).await?;

                Ok(())
            }
            Err(s) => {
                let flags: usize = usize::MAX;
                write_varint(io, flags).await?;
                write_length_prefixed(io, s).await?;

                Ok(())
            }
        }
    }
}

pub fn handle_event(
    event: RequestResponseEvent<SimpleFileRequest, Result<SimpleFileResponse, String>>,
    swarm: &mut Swarm<ComposedBehaviour>,
) {
    match event {
        RequestResponseEvent::Message { peer, message } => match message {
            RequestResponseMessage::Request {
                request_id,
                request,
                channel,
            } => {
                log::debug!(
                    "RequestResponseMessage::Request {} {} {:?}",
                    request_id,
                    peer,
                    request
                );

                let b = swarm.behaviour_mut();
                let key = libp2p::kad::record::Key::new(&request.key);

                match b.state.published_files_mapping.get(&key).cloned() {
                    Some(filename) => b
                        .event_sender
                        .send(InternalNetworkEvent::InboundFileRequest {
                            peer,
                            request,
                            filename,
                            channel,
                        })
                        .expect("receiver to be receiving"),
                    None => warn!(
                        "Received request but no file published for key {:?}",
                        request.key
                    ),
                };
            }
            RequestResponseMessage::Response {
                request_id,
                response,
            } => {
                log::debug!("RequestResponseMessage::Response {} {}", request_id, peer);
                swarm
                    .behaviour_mut()
                    .state
                    .pending_simple_file_request
                    .remove(&request_id)
                    .expect("Request to still be pending")
                    .send(Ok(response))
                    .expect("Receiver not to be dropped")
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
            swarm
                .behaviour_mut()
                .state
                .pending_simple_file_request
                .remove(&request_id)
                .expect("Request to still be pending")
                .send(utils::to_simple_error(error))
                .expect("Receiver not to be dropped");
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
