// SPDX-License-Identifier: AGPL-3.0-only


use async_trait::async_trait;
use futures::{io, AsyncRead, AsyncWrite};
use libp2p::core::upgrade::read_varint;
use libp2p::core::ProtocolName;
use libp2p::request_response::{
    RequestResponseCodec, RequestResponseEvent, RequestResponseMessage, ResponseChannel,
};
use libp2p::{PeerId, Swarm};
use log::warn;

use super::media_streaming::{self, StreamingKey};
use super::ComposedBehaviour;
use crate::utils;

#[derive(Clone)]
pub struct DelegatedStreamingProtocol();
#[derive(Clone)]
pub struct DelegatedStreamingCodec();

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct WebRtcStream(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WebRtcTrack {
    pub stream_id: WebRtcStream,
    pub track_id: String,
}

#[derive(Debug, Clone)]
pub struct PublishStreamInfo {
    pub streaming_key: StreamingKey,
    pub tracks: Vec<WebRtcTrack>,
}

#[derive(Debug)]
pub struct PlayStreamNewTrackInfo {
    pub streaming_key: StreamingKey,
    pub tracks: Vec<WebRtcTrack>,
}

#[derive(Debug)]
pub enum DelegatedStreamingRequest {
    PublishStream(PublishStreamInfo),
    PlayStream { streaming_key: StreamingKey },
    PlayStreamNewTrack(PlayStreamNewTrackInfo),
}

#[derive(Debug, Default)]
pub struct DelegatedStreamingResponse {}

pub struct RequestEvent {
    pub peer: PeerId,
    pub request: DelegatedStreamingRequest,
    pub channel: ResponseChannel<DelegatedStreamingResponseResult>,
}

pub type DelegatedStreamingResponseResult = Result<DelegatedStreamingResponse, String>;

impl ProtocolName for DelegatedStreamingProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/delegated-streaming/1".as_bytes()
    }
}

const MAX_SIZE: usize = 100_000;

#[async_trait]
impl RequestResponseCodec for DelegatedStreamingCodec {
    type Protocol = DelegatedStreamingProtocol;
    type Request = DelegatedStreamingRequest;
    type Response = DelegatedStreamingResponseResult;

    async fn read_request<T>(
        &mut self,
        _: &DelegatedStreamingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let i = read_varint(io).await?;
        let streaming_key = media_streaming::deserialize_streaming_key(io).await?;
        match i {
            1 => {
                let tracks = deserialize_webrtc_tracks(io).await?;
                Ok(DelegatedStreamingRequest::PublishStream(
                    PublishStreamInfo {
                        streaming_key,
                        tracks,
                    },
                ))
            }
            2 => Ok(DelegatedStreamingRequest::PlayStream { streaming_key }),
            3 => {
                let tracks = deserialize_webrtc_tracks(io).await?;
                Ok(DelegatedStreamingRequest::PlayStreamNewTrack(
                    PlayStreamNewTrackInfo {
                        streaming_key,
                        tracks,
                    },
                ))
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Error reading DelegatedStreamingRequest: {other} is a invalid code"),
            )),
        }
    }

    async fn read_response<T>(
        &mut self,
        _: &DelegatedStreamingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        match read_varint(io).await? {
            1 => {
                let s =
                    utils::read_to_string(io, MAX_SIZE, "DelegatedStreamingProtocol error string")
                        .await?;
                Ok(Err(s))
            }
            2 => Ok(Ok(DelegatedStreamingResponse {})),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Error reading DelegatedStreamingResponse: {other} is a invalid code"),
            )),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &DelegatedStreamingProtocol,
        io: &mut T,
        r: DelegatedStreamingRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let (i, streaming_key) = match &r {
            DelegatedStreamingRequest::PublishStream(PublishStreamInfo {
                streaming_key, ..
            }) => (1, streaming_key),
            DelegatedStreamingRequest::PlayStream { streaming_key } => (2, streaming_key),
            DelegatedStreamingRequest::PlayStreamNewTrack(PlayStreamNewTrackInfo {
                streaming_key,
                ..
            }) => (3, streaming_key),
        };
        utils::write_varint(io, i).await?;
        media_streaming::serialize_streaming_key(io, streaming_key).await?;
        match r {
            DelegatedStreamingRequest::PlayStream { .. } => {}
            DelegatedStreamingRequest::PublishStream(PublishStreamInfo {
                tracks,
                streaming_key: _,
            }) => {
                serialize_webrtc_tracks(io, &tracks).await?;
            }
            DelegatedStreamingRequest::PlayStreamNewTrack(PlayStreamNewTrackInfo {
                tracks,
                streaming_key: _,
            }) => {
                serialize_webrtc_tracks(io, &tracks).await?;
            }
        };


        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &DelegatedStreamingProtocol,
        io: &mut T,
        r: DelegatedStreamingResponseResult,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        match r {
            Err(e) => {
                utils::write_varint(io, 1).await?;
                utils::write_limited_length_prefixed(io, e.as_bytes(), MAX_SIZE).await?;
            }
            Ok(_) => {
                utils::write_varint(io, 2).await?;
            }
        };
        Ok(())
    }
}

pub(crate) fn handle_event(
    event: RequestResponseEvent<DelegatedStreamingRequest, DelegatedStreamingResponseResult>,
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
                if let Err(e) =
                    swarm
                        .behaviour_mut()
                        .delegated_streaming_sender
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
                    .pending_delegated_streaming_request
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
                .pending_delegated_streaming_request
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

async fn deserialize_webrtc_tracks<T: AsyncRead + Unpin + Send>(
    io: &mut T,
) -> io::Result<Vec<WebRtcTrack>> {
    let tracks_size = read_varint(io).await?;
    let mut tracks = Vec::with_capacity(tracks_size);
    for _ in 0..tracks_size {
        let stream_id = utils::read_to_string(
            io,
            MAX_SIZE,
            "DelegatedStreamingProtocol WebRtcTrack stream_id",
        )
        .await?;
        let track_id = utils::read_to_string(
            io,
            MAX_SIZE,
            "DelegatedStreamingProtocol WebRtcTrack track_id",
        )
        .await?;
        let track = WebRtcTrack {
            stream_id: WebRtcStream(stream_id),
            track_id,
        };
        tracks.push(track);
    }
    Ok(tracks)
}

async fn serialize_webrtc_tracks<T: AsyncWrite + Unpin + Send>(
    io: &mut T,
    tracks: &Vec<WebRtcTrack>,
) -> io::Result<usize> {
    let mut n = 0;
    n += utils::write_varint(io, tracks.len()).await?;
    for track in tracks {
        n += utils::write_limited_length_prefixed(io, track.stream_id.0.as_bytes(), MAX_SIZE)
            .await?;
        n += utils::write_limited_length_prefixed(io, track.track_id.as_bytes(), MAX_SIZE).await?;
    }
    Ok(n)
}
