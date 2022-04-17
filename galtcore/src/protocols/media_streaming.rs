// SPDX-License-Identifier: AGPL-3.0-only


use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use libp2p::core::upgrade::{read_length_prefixed, read_varint};
use libp2p::core::ProtocolName;
use libp2p::futures::{io, AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::identity::Keypair;
use libp2p::request_response::{
    RequestResponseCodec, RequestResponseEvent, RequestResponseMessage,
};
use libp2p::{PeerId, Swarm};
use log::warn;
use serde::{Deserialize, Serialize};

use super::kademlia_record::StreamingRecord;
use super::{ComposedBehaviour, InternalNetworkEvent};
use crate::daemons::internal_network_events::BroadcastableNetworkEvent;
use crate::utils;

#[derive(Debug, Clone)]
pub struct StreamingProtocol();
#[derive(Clone)]
pub struct StreamingCodec {
    pub broadcast_event_sender: tokio::sync::broadcast::Sender<BroadcastableNetworkEvent>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StreamingKey {
    pub channel_key: PeerId,
    pub video_key: Vec<u8>,
}

impl std::fmt::Debug for StreamingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingKey")
            .field("channel_key", &self.channel_key)
            .field("video_key", &bs58::encode(&self.video_key).into_string())
            .finish()
    }
}

impl std::fmt::Display for StreamingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "{}/{}",
            self.channel_key,
            bs58::encode(&self.video_key).into_string(),
        ))
    }
}

impl TryFrom<&str> for StreamingKey {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = value.trim();
        if value.is_empty() {
            anyhow::bail!("Empty input string");
        }
        let splitted = value.split('/').collect_vec();
        if splitted.len() != 2 {
            anyhow::bail!(
                "Expected two string separated by '/' but got {} strings",
                splitted.len()
            );
        }

        let channel_key = splitted[0].trim();
        if channel_key.is_empty() {
            anyhow::bail!("channel_key is empty");
        }
        let channel_key = bs58::decode(&channel_key)
            .into_vec()
            .context("error decoding channel key")?;

        let channel_key: PeerId =
            PeerId::from_bytes(&channel_key).context("Error converting channel key to peer id")?;

        let video_key = splitted[1].trim();
        if video_key.is_empty() {
            anyhow::bail!("video_key is empty");
        }
        let video_key = bs58::decode(&video_key)
            .into_vec()
            .context("error decoding video key")?;

        Ok(StreamingKey {
            video_key,
            channel_key,
        })
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct StreamOffset {
    pub timestamp_reference: u32,
    pub sequence_id: u32,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum StreamSeekType {
    Reset,
    Offset(StreamOffset),
    Peek,
}

impl StreamingKey {
    pub(crate) fn hash_from_parts(video_key: &[u8], channel_key: &PeerId) -> Vec<u8> {
        blake3::hash(
            [
                blake3::hash(&channel_key.to_bytes()).as_bytes().as_slice(),
                blake3::hash(video_key).as_bytes().as_slice(),
            ]
            .concat()
            .as_slice(),
        )
        .as_bytes()
        .to_vec()
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct StreamingRequest {
    pub streaming_key: StreamingKey,
    pub seek_type: StreamSeekType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamingDataType {
    RtmpAudio,
    RtmpVideo,
    WebRtcRtpPacket,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamTrack {
    pub stream_id: u8,
    pub track_id: u8,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MyRTCPFeedback {
    pub typ: String,
    pub parameter: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MyRTCRtpCodecCapability {
    pub mime_type: String,
    pub clock_rate: u32,
    pub channels: u16,
    pub sdp_fmtp_line: String,
    pub rtcp_feedback: Vec<MyRTCPFeedback>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    pub capability: MyRTCRtpCodecCapability,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingData {
    pub data: Vec<u8>,
    pub stream_track: StreamTrack,
    pub metadata: Option<StreamMetadata>,
    pub source_offset: StreamOffset,
    pub data_type: StreamingDataType,
}

impl StreamingData {
    pub(crate) fn hash(&self, salt: &[u8]) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(salt);
        match self.data_type {
            StreamingDataType::RtmpAudio => hasher.update(&[0u8]),
            StreamingDataType::RtmpVideo => hasher.update(&[1u8]),
            StreamingDataType::WebRtcRtpPacket => hasher.update(&[2u8]),
        };
        hasher.update(&self.data);
        hasher.update(&self.source_offset.sequence_id.to_be_bytes());
        hasher.update(&self.source_offset.timestamp_reference.to_be_bytes());
        hasher.update(salt);
        hasher.finalize()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedStreamingData {
    pub streaming_data: StreamingData,
    pub signature: Vec<u8>,
}

impl SignedStreamingData {
    pub fn from(
        data: StreamingData,
        keypair: &Keypair,
        record: &StreamingRecord,
    ) -> anyhow::Result<Self> {
        let hash = data.hash(record.key.as_ref());
        let signature = keypair.sign(hash.as_bytes())?;
        Ok(SignedStreamingData {
            streaming_data: data,
            signature,
        })
    }

    pub(crate) fn verify(&self, record: &StreamingRecord) -> bool {
        let hash = self.streaming_data.hash(record.key.as_ref());
        record.public_key.verify(hash.as_bytes(), &self.signature)
    }
}

#[derive(Debug)]
pub enum RTMPFrameType {
    VideoSequenceHeader,
    VideoKeyframe,
    Video,
    AudioSequenceHeader,
    KeyAudio,
    Invalid,
    Other,
}

impl RTMPFrameType {
    pub fn classify(data: &[u8]) -> RTMPFrameType {
        if data.len() >= 2 {
            match data[0] {
                0x17 => match data[1] {
                    0x00 => Self::VideoSequenceHeader,
                    _ => Self::VideoKeyframe,
                },
                0xaf => match data[1] {
                    // AAC
                    0x00 => Self::AudioSequenceHeader,
                    _ => Self::KeyAudio,
                },
                0x2f => match data[1] {
                    // MP3
                    0x00 => Self::AudioSequenceHeader,
                    _ => Self::KeyAudio,
                },
                0x27 => Self::Video,
                other => {
                    log::debug!("Received RTMPFrameType::Other = {other:#04x}");
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
#[must_use]
pub enum StreamingResponse {
    Data(Vec<SignedStreamingData>), // TODO: something should test for the soundness of the returned data (i.e have headers when necessary, no frames missing)
    MaxUploadRateReached,
    TooMuchFlood,
}

#[derive(Debug)]
#[must_use]
pub struct WrappedStreamingResponseResult {
    pub peer: Option<PeerId>,
    pub response: Result<StreamingResponse, String>,
}

pub type StreamingResponseResult = Result<StreamingResponse, String>;

impl ProtocolName for StreamingProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/media-streaming/1".as_bytes()
    }
}
const MAX_SIZE: usize = 10_000_000;


pub(crate) async fn deserialize_streaming_key<T: AsyncRead + Unpin + Send>(
    io: &mut T,
) -> io::Result<StreamingKey> {
    let channel_key = read_length_prefixed(io, 1_000).await?;
    if channel_key.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Empty channel key",
        ));
    }
    let channel_key = match channel_key.try_into() {
        Ok(p) => p,
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid channel key",
            ))
        }
    };
    let video_key = read_length_prefixed(io, 1_000).await?;
    if video_key.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Empty video key",
        ));
    }
    Ok(StreamingKey {
        video_key,
        channel_key,
    })
}

pub(crate) async fn serialize_streaming_key<T: AsyncWrite + Unpin + Send>(
    io: &mut T,
    streaming_key: &StreamingKey,
) -> io::Result<usize> {
    let mut n = 0;
    n += utils::write_limited_length_prefixed(io, streaming_key.channel_key.to_bytes(), MAX_SIZE)
        .await?;
    n += utils::write_limited_length_prefixed(io, &streaming_key.video_key, MAX_SIZE).await?;
    Ok(n)
}

#[async_trait]
impl RequestResponseCodec for StreamingCodec {
    type Protocol = StreamingProtocol;
    type Request = StreamingRequest;
    type Response = WrappedStreamingResponseResult;

    async fn read_request<T>(
        &mut self,
        _: &StreamingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let seek_type = match (read_varint(io).await? as u32, read_varint(io).await? as u32) {
            (u32::MAX, u32::MAX) => StreamSeekType::Reset,
            (u32::MAX, 0) => StreamSeekType::Peek,
            (timestamp_reference, sequence_id) => StreamSeekType::Offset(StreamOffset {
                timestamp_reference,
                sequence_id,
            }),
        };

        let streaming_key = deserialize_streaming_key(io).await?;

        Ok(StreamingRequest {
            streaming_key,
            seek_type,
        })
    }

    async fn read_response<T>(
        &mut self,
        _: &StreamingProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // TODO: count bytes per peer
        let result = match read_varint(io).await? as u8 {
            1 => {
                let data =
                    utils::read_to_string(io, 10_000, "StreamingProtocol error string").await;
                match data {
                    Ok(s) => Ok(Err(s)),
                    Err(e) => Err(e),
                }
            }
            2 => match read_varint(io).await? as usize {
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
                        let data = read_length_prefixed(io, MAX_SIZE).await?;
                        let r = match bincode::deserialize(&data) {
                            Ok(r) => r,
                            Err(e) => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("Error deserializing response {e:?}"),
                                ))
                            }
                        };
                        responses.push(r);
                        // let flags = read_varint(io).await?;
                        // let data = read_length_prefixed(io, 10_000_000).await?.into();
                        // let timestamp = read_varint(io).await? as u32;
                        // let timestamp_reference = read_varint(io).await? as u32;
                        // let sequence_id = read_varint(io).await? as u32;
                        // let signature = read_length_prefixed(io, 1_000).await?.into();

                        // let data_type = match flags {
                        //     1 => StreamingDataType::RtmpVideo,
                        //     2 => StreamingDataType::RtmpAudio,
                        //     3 => StreamingDataType::WebRtcRtpPacket,
                        //     other => {
                        //         return Err(std::io::Error::new(
                        //             std::io::ErrorKind::InvalidData,
                        //             format!("Invalid streaming data type: {other}"),
                        //         ))
                        //     }
                        // };

                        // let streaming_data = StreamingData {
                        //     data,
                        //     timestamp,
                        //     source_offset: StreamOffset {
                        //         timestamp_reference,
                        //         sequence_id,
                        //     },
                        //     data_type,
                        // };
                        // let signed_data = SignedStreamingData {
                        //     streaming_data,
                        //     signature,
                        // };
                        // responses.push(signed_data);
                    }
                    Ok(Ok(StreamingResponse::Data(responses)))
                }
            },
            3 => Ok(Ok(StreamingResponse::MaxUploadRateReached)),
            4 => Ok(Ok(StreamingResponse::TooMuchFlood)),
            n => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Received RTMPStreamingResponse code {}", n),
            )),
        };

        match result {
            Ok(response) => Ok(WrappedStreamingResponseResult {
                peer: None,
                response,
            }),
            Err(e) => Err(e),
        }
    }

    async fn write_request<T>(
        &mut self,
        _: &StreamingProtocol,
        io: &mut T,
        r: StreamingRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let (timestamp_reference, sequence_id) = match r.seek_type {
            StreamSeekType::Reset => (u32::MAX, u32::MAX),
            StreamSeekType::Peek => (u32::MAX, 0),
            StreamSeekType::Offset(StreamOffset {
                timestamp_reference,
                sequence_id,
            }) => (timestamp_reference, sequence_id),
        };
        utils::write_varint(io, timestamp_reference as usize).await?;
        utils::write_varint(io, sequence_id as usize).await?;
        serialize_streaming_key(io, &r.streaming_key).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamingProtocol,
        io: &mut T,
        r: WrappedStreamingResponseResult,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let mut written_bytes = 0;
        let mut responses_count: u32 = 0;
        let start = instant::Instant::now();
        let result = match r.response {
            Err(s) => {
                written_bytes += utils::write_varint(io, 1).await?;
                written_bytes += utils::write_limited_length_prefixed(io, s, MAX_SIZE).await?;
                responses_count += 1;
                Ok(())
            }
            Ok(StreamingResponse::Data(responses))
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
            Ok(StreamingResponse::Data(responses)) => {
                written_bytes += utils::write_varint(io, 2).await?;
                written_bytes += utils::write_varint(io, responses.len()).await?;
                for r in responses {
                    match bincode::serialize(&r) {
                        Ok(r) => {
                            written_bytes +=
                                utils::write_limited_length_prefixed(io, r, MAX_SIZE).await?;
                        }
                        Err(e) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Error serializing response {e:?}"),
                            ))
                        }
                    }
                    responses_count += 1;
                    // let flags: usize = match r.streaming_data.data_type {
                    //     StreamingDataType::RtmpAudio => 0,
                    //     StreamingDataType::RtmpVideo => 1,
                    //     StreamingDataType::WebRtcRtpPacket => 2,
                    // }; // TODO: optimize and pack these flags in fewer bits
                    // written_bytes += utils::write_varint(io, flags).await?;
                    // written_bytes +=
                    //     utils::write_limited_length_prefixed(io, r.streaming_data.data, MAX_SIZE)
                    //         .await?;
                    // written_bytes += utils::write_varint(
                    //     io,
                    //     r.streaming_data.source_offset.timestamp_reference as usize,
                    // )
                    // .await?;
                    // written_bytes += utils::write_varint(
                    //     io,
                    //     r.streaming_data.source_offset.sequence_id as usize,
                    // )
                    // .await?;

                    // written_bytes +=
                    //     utils::write_limited_length_prefixed(io, r.signature, MAX_SIZE).await?;
                    // responses_count += 1;
                }
                Ok(())
            }
            Ok(StreamingResponse::MaxUploadRateReached) => {
                written_bytes += utils::write_varint(io, 3).await?;
                responses_count += 1;
                Ok(())
            }
            Ok(StreamingResponse::TooMuchFlood) => {
                written_bytes += utils::write_varint(io, 4).await?;
                responses_count += 1;
                Ok(())
            }
        };
        io.flush().await?;
        let finish = instant::Instant::now();
        let write_duration = finish.duration_since(start);
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
            "SentStreamingResponseStats written_bytes {written_bytes} responses_count {responses_count} ({avg_response_size}B/message) write_duration {write_duration:?} {rate_kb:.2}kB/s"
        );
        if let Err(e) = self.broadcast_event_sender.send(
            BroadcastableNetworkEvent::SentStreamingResponseStats {
                peer: r.peer.expect("code to not be bugged"),
                now: finish,
                written_bytes,
                write_duration,
                responses_count,
            },
        ) {
            log::warn!("Error sending SentStreamingResponseStats: {}", e);
        }
        result
    }
}

pub(crate) fn handle_event(
    event: RequestResponseEvent<StreamingRequest, WrappedStreamingResponseResult>,
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
                    .send(InternalNetworkEvent::InboundStreamingDataRequest {
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
                    .pending_streaming_request
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
                .pending_streaming_request
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_key_from_str_and_back() -> anyhow::Result<()> {
        let s = "12D3KooWQYhTNQdmr3ArTeUHRYzFg94BKyTkoWBDWez9kSCVe2Xo/5ezFfhtXGXtJcodB6E3Fu3cXTKkYERTGDH5x2JADMTZk";
        let k: StreamingKey = s.try_into()?;
        assert_eq!(s, k.to_string());
        Ok(())
    }
}
