// SPDX-License-Identifier: AGPL-3.0-only

use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use libp2p::core::upgrade::{read_length_prefixed, read_varint};
use libp2p::core::ProtocolName;
use libp2p::futures::{io, AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::identity::Keypair;
use libp2p::request_response::{
    RequestResponseCodec, RequestResponseEvent, RequestResponseMessage,
};
use libp2p::{PeerId, Swarm};
use log::warn;

use super::kademlia_record::RtmpStreamingRecord;
use super::{ComposedBehaviour, InternalNetworkEvent};
use crate::utils;

#[derive(Debug, Clone)]
pub struct RTMPStreamingProtocol();
#[derive(Clone)]
pub struct RTMPStreamingCodec {
    pub event_sender: tokio::sync::mpsc::UnboundedSender<InternalNetworkEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RtmpStreamingKey {
    pub app_name: String, // FIXME: sanitize this
    pub stream_key: PeerId,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct StreamOffset {
    pub timestamp_reference: u32,
    pub sequence_id: u32,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum RTMPDataSeekType {
    Reset,
    Offset(StreamOffset),
    Peek,
}

impl RtmpStreamingKey {
    pub fn hash_from_parts(app_name: &[u8], stream_key: &PeerId) -> Vec<u8> {
        blake3::hash(
            [
                blake3::hash(app_name).as_bytes().as_slice(),
                blake3::hash(&stream_key.to_bytes()).as_bytes().as_slice(),
            ]
            .concat()
            .as_slice(),
        )
        .as_bytes()
        .to_vec()
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RTMPStreamingRequest {
    pub streaming_key: RtmpStreamingKey,
    pub seek_type: RTMPDataSeekType,
}

#[derive(Debug, Clone)]
pub enum RtmpDataType {
    Audio,
    Video,
}

#[derive(Debug, Clone)]
pub struct RtmpData {
    pub data: Bytes,
    pub rtmp_timestamp: u32,
    pub source_offset: StreamOffset,
    pub data_type: RtmpDataType,
}

impl RtmpData {
    pub fn hash(&self, salt: &[u8]) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(salt);
        match self.data_type {
            RtmpDataType::Audio => hasher.update(&[0u8]),
            RtmpDataType::Video => hasher.update(&[1u8]),
        };
        hasher.update(&self.data);
        hasher.update(&self.rtmp_timestamp.to_be_bytes());
        hasher.update(&self.source_offset.sequence_id.to_be_bytes());
        hasher.update(&self.source_offset.timestamp_reference.to_be_bytes());
        hasher.update(salt);
        hasher.finalize()
    }
}

#[derive(Debug, Clone)]
pub struct SignedRtmpData {
    pub rtmp_data: RtmpData,
    pub signature: Bytes,
}

impl SignedRtmpData {
    pub fn from(
        data: RtmpData,
        keypair: &Keypair,
        record: &RtmpStreamingRecord,
    ) -> anyhow::Result<Self> {
        let hash = data.hash(record.key.as_ref());
        let signature = keypair.sign(hash.as_bytes())?;
        let signature = Bytes::from(signature);
        Ok(SignedRtmpData {
            rtmp_data: data,
            signature,
        })
    }

    pub fn verify(&self, record: &RtmpStreamingRecord) -> bool {
        let hash = self.rtmp_data.hash(record.key.as_ref());
        record.public_key.verify(hash.as_bytes(), &self.signature)
    }
}

#[derive(Debug)]
pub enum RTMPFrameType {
    VideoSequenceHeader,
    VideoKeyframe,
    AudioSequenceHeader,
    KeyAudio,
    Invalid,
    Other,
}

impl RTMPFrameType {
    pub fn frame_type(data: &Bytes) -> RTMPFrameType {
        if data.len() >= 2 {
            match data[0] {
                0x17 => match data[1] {
                    0x00 => Self::VideoSequenceHeader,
                    _ => Self::VideoKeyframe,
                },
                0xaf => match data[1] {
                    0x00 => Self::AudioSequenceHeader,
                    _ => Self::KeyAudio,
                },
                other => {
                    log::debug!("Received RTMPFrameType:Other = {other}");
                    Self::Other
                }
            }
        } else {
            Self::Invalid
        }
    }
}

pub const MAX_RESPONSES_PER_REQUEST: usize = 200;

#[derive(Debug, Clone)]
pub enum RtmpStreamingResponse {
    Data(Vec<SignedRtmpData>), // TODO: something should test for the soundness of the returned data (i.e have headers when necessary, no frames missing)
    MaxUploadRateReached,
    TooMuchFlood,
}

#[derive(Debug)]
pub struct WrappedRTMPStreamingResponseResult {
    pub peer: Option<PeerId>,
    pub response: Result<RtmpStreamingResponse, String>,
}

pub type RTMPStreamingResponseResult = Result<RtmpStreamingResponse, String>;

impl ProtocolName for RTMPStreamingProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/rtmp-streaming/1".as_bytes()
    }
}

#[async_trait]
impl RequestResponseCodec for RTMPStreamingCodec {
    type Protocol = RTMPStreamingProtocol;
    type Request = RTMPStreamingRequest;
    type Response = WrappedRTMPStreamingResponseResult;

    async fn read_request<T>(
        &mut self,
        _: &RTMPStreamingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let seek_type = match (read_varint(io).await? as u32, read_varint(io).await? as u32) {
            (u32::MAX, u32::MAX) => RTMPDataSeekType::Reset,
            (u32::MAX, 0) => RTMPDataSeekType::Peek,
            (timestamp_reference, sequence_id) => RTMPDataSeekType::Offset(StreamOffset {
                timestamp_reference,
                sequence_id,
            }),
        };

        let app_name = read_length_prefixed(io, 1_000).await?;
        if app_name.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty app name",
            ));
        }
        let stream_key = read_length_prefixed(io, 1_000).await?;
        if stream_key.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty stream_key",
            ));
        }
        let app_name = std::str::from_utf8(&app_name).map_err(crate::utils::utf8_error)?;
        let stream_key = match stream_key.try_into() {
            Ok(p) => p,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid stream_key",
                ))
            }
        };
        Ok(RTMPStreamingRequest {
            streaming_key: RtmpStreamingKey {
                app_name: app_name.to_string(),
                stream_key,
            },
            seek_type,
        })
    }

    async fn read_response<T>(
        &mut self,
        _: &RTMPStreamingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // TODO: count bytes per peer
        let result = match read_varint(io).await? as u8 {
            0 => {
                let data = read_length_prefixed(io, 10_000).await?;
                match std::str::from_utf8(&data) {
                    Ok(s) => Ok(Err(s.into())),
                    Err(e) => Err(crate::utils::utf8_error(e)),
                }
            }
            1 => match read_varint(io).await? as usize {
                n if n > MAX_RESPONSES_PER_REQUEST => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Too many responses: {} max is: {}",
                        n, MAX_RESPONSES_PER_REQUEST
                    ),
                )),
                responses_len => {
                    let mut responses = Vec::with_capacity(responses_len);
                    for _ in 0..responses_len {
                        let flags = read_varint(io).await?;
                        let data = read_length_prefixed(io, 10_000_000).await?.into();
                        let rtmp_timestamp = read_varint(io).await? as u32;
                        let timestamp_reference = read_varint(io).await? as u32;
                        let sequence_id = read_varint(io).await? as u32;
                        let signature = read_length_prefixed(io, 1_000).await?.into();

                        let data_type = if flags == 1 {
                            RtmpDataType::Video
                        } else {
                            RtmpDataType::Audio
                        };

                        let rtmp_data = RtmpData {
                            data,
                            rtmp_timestamp,
                            source_offset: StreamOffset {
                                timestamp_reference,
                                sequence_id,
                            },
                            data_type,
                        };
                        let signed_data = SignedRtmpData {
                            rtmp_data,
                            signature,
                        };
                        responses.push(signed_data);
                    }
                    Ok(Ok(RtmpStreamingResponse::Data(responses)))
                }
            },
            2 => Ok(Ok(RtmpStreamingResponse::MaxUploadRateReached)),
            3 => Ok(Ok(RtmpStreamingResponse::TooMuchFlood)),
            n => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received RTMPStreamingResponse code {}", n),
            )),
        };

        match result {
            Ok(response) => Ok(WrappedRTMPStreamingResponseResult {
                peer: None,
                response,
            }),
            Err(e) => Err(e),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &RTMPStreamingProtocol,
        io: &mut T,
        r: RTMPStreamingRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let (timestamp_reference, sequence_id) = match r.seek_type {
            RTMPDataSeekType::Reset => (u32::MAX, u32::MAX),
            RTMPDataSeekType::Peek => (u32::MAX, 0),
            RTMPDataSeekType::Offset(StreamOffset {
                timestamp_reference,
                sequence_id,
            }) => (timestamp_reference, sequence_id),
        };
        utils::write_varint(io, timestamp_reference as usize).await?;
        utils::write_varint(io, sequence_id as usize).await?;
        utils::write_length_prefixed(io, r.streaming_key.app_name.as_bytes()).await?;
        utils::write_length_prefixed(io, r.streaming_key.stream_key.to_bytes()).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &RTMPStreamingProtocol,
        io: &mut T,
        r: WrappedRTMPStreamingResponseResult,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let mut written_bytes = 0;
        let mut responses_count: u32 = 0;
        let start = SystemTime::now();
        let result = match r.response {
            Err(s) => {
                written_bytes += utils::write_varint(io, 0).await?;
                written_bytes += utils::write_length_prefixed(io, s).await?;
                responses_count += 1;
                Ok(())
            }
            Ok(RtmpStreamingResponse::Data(responses))
                if responses.len() > MAX_RESPONSES_PER_REQUEST =>
            {
                responses_count += 1;
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Too many responses: {} max is: {}",
                        responses.len(),
                        MAX_RESPONSES_PER_REQUEST
                    ),
                ))
            }
            Ok(RtmpStreamingResponse::Data(responses)) => {
                written_bytes += utils::write_varint(io, 1).await?;
                written_bytes += utils::write_varint(io, responses.len()).await?;
                for r in responses {
                    let flags: usize = match r.rtmp_data.data_type {
                        RtmpDataType::Audio => 0,
                        RtmpDataType::Video => 1,
                    }; // TODO: optimize and pack these flags in fewer bits
                    written_bytes += utils::write_varint(io, flags).await?;
                    written_bytes += utils::write_length_prefixed(io, r.rtmp_data.data).await?;
                    written_bytes +=
                        utils::write_varint(io, r.rtmp_data.rtmp_timestamp as usize).await?;
                    written_bytes += utils::write_varint(
                        io,
                        r.rtmp_data.source_offset.timestamp_reference as usize,
                    )
                    .await?;
                    written_bytes +=
                        utils::write_varint(io, r.rtmp_data.source_offset.sequence_id as usize)
                            .await?;

                    written_bytes += utils::write_length_prefixed(io, r.signature).await?;
                    responses_count += 1;
                }
                Ok(())
            }
            Ok(RtmpStreamingResponse::MaxUploadRateReached) => {
                written_bytes += utils::write_varint(io, 2).await?;
                responses_count += 1;
                Ok(())
            }
            Ok(RtmpStreamingResponse::TooMuchFlood) => {
                written_bytes += utils::write_varint(io, 3).await?;
                responses_count += 1;
                Ok(())
            }
        };
        io.flush().await?;
        let finish = SystemTime::now();
        let write_duration = finish.duration_since(start).expect("time to work");
        let micros = write_duration.as_micros();
        let rate_kb = if micros > 0 {
            1000.0 * written_bytes as f64 / micros as f64
        } else {
            0.0
        };
        let avg_response_size = if responses_count > 0 {
            written_bytes / responses_count as usize
        } else {
            0
        };
        log::debug!(
            "SentRTMPResponseBytes written_bytes {written_bytes} responses_count {responses_count} ({avg_response_size}B/message) write_duration {write_duration:?} {rate_kb:.2}kB/s"
        );
        if let Err(e) = self
            .event_sender
            .send(InternalNetworkEvent::SentRTMPResponseStats {
                peer: r.peer.expect("code to not be bugged"),
                now: finish,
                written_bytes,
                write_duration,
                responses_count,
            })
        {
            log::warn!("Error sending SentRTMPResponseBytes: {}", e);
        }
        result
    }
}

pub fn handle_event(
    event: RequestResponseEvent<RTMPStreamingRequest, WrappedRTMPStreamingResponseResult>,
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
                swarm
                    .behaviour_mut()
                    .event_sender
                    .send(InternalNetworkEvent::InboundRTMPStreamingDataRequest {
                        peer,
                        request,
                        channel,
                    })
                    .expect("receiver to be receiving");
            }
            RequestResponseMessage::Response {
                request_id,
                response,
            } => {
                log::debug!("RequestResponseMessage::Response {} {}", request_id, peer);
                if swarm
                    .behaviour_mut()
                    .state
                    .pending_rtmp_streaming_request
                    .remove(&request_id)
                    .expect("Request to still be pending")
                    .send(Ok(response.response))
                    .is_err()
                {
                    log::warn!("Unexpected drop of receiver")
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
                .pending_rtmp_streaming_request
                .remove(&request_id)
                .expect("Request to still be pending")
                .send(Err(error))
            {
                log::warn!("Receiver dropped while trying to send: {e:?}")
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
