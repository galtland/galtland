// SPDX-License-Identifier: AGPL-3.0-only

use std::cmp::max;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use galtcore::cm::modules::streaming::stream_publisher::StreamPublisherClient;
use galtcore::cm::modules::streaming::{PlayStreamInfo, PublishStreamInfo};
use galtcore::cm::{self, SharedGlobalState};
use galtcore::networkbackendclient::NetworkBackendClient;
use galtcore::protocols::kademlia_record::StreamingRecord;
use galtcore::protocols::media_streaming::{
    IntegerStreamTrack, RTMPFrameType, RtmpMetadata, StreamMetadata, StreamOffset, StreamSeekType,
    StreamingData, StreamingDataType, StreamingKey, StreamingResponse, VerifiedSignedStreamingData,
};
use galtcore::protocols::NodeIdentity;
use galtcore::utils::spawn_and_log_error;
use libp2p::bytes::BytesMut;
use libp2p::PeerId;
use rml_rtmp::chunk_io::Packet;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionError, ServerSessionEvent, ServerSessionResult,
};
use rml_rtmp::time::RtmpTimestamp;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;
use webrtc::api::media_engine::MIME_TYPE_H264;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::rtp_transceiver::RTCPFeedback;


pub async fn accept_loop(
    addr: SocketAddr,
    identity: NodeIdentity,
    shared_global_state: SharedGlobalState,
    network: NetworkBackendClient,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    log::info!("RTMP Server is listening to {}", addr);
    loop {
        log::debug!("Looping accept loop");
        match listener.accept().await {
            Ok((stream, address)) => {
                log::info!("New connection: {}", address);
                let identity = identity.clone();
                spawn_and_log_error(connection_loop(
                    stream,
                    identity,
                    shared_global_state.clone(),
                    network.clone(),
                ));
            }
            Err(e) => {
                log::warn!("Error accepting new RTMP connection: {}", e);
            }
        }
    }
}

fn clone(p: &rml_rtmp::chunk_io::Packet) -> rml_rtmp::chunk_io::Packet {
    rml_rtmp::chunk_io::Packet {
        bytes: p.bytes.clone(),
        can_be_dropped: p.can_be_dropped,
    }
}

async fn connection_loop(
    mut stream: TcpStream,
    identity: NodeIdentity,
    shared_global_state: SharedGlobalState,
    mut network: NetworkBackendClient,
) -> anyhow::Result<()> {
    let capability = RTCRtpCodecCapability {
        mime_type: MIME_TYPE_H264.to_owned(),
        clock_rate: 90000,
        channels: 0,
        sdp_fmtp_line: "" // "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f"
            .to_owned(),
        rtcp_feedback: vec![
            RTCPFeedback {
                typ: "goog-remb".to_owned(),
                parameter: "".to_owned(),
            },
            RTCPFeedback {
                typ: "ccm".to_owned(),
                parameter: "fir".to_owned(),
            },
            RTCPFeedback {
                typ: "nack".to_owned(),
                parameter: "".to_owned(),
            },
            RTCPFeedback {
                typ: "nack".to_owned(),
                parameter: "pli".to_owned(),
            },
        ],
    };

    // let udp_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    // udp_socket.connect("127.0.0.1:1567").await?;

    // let mut h264_processor = crate::media::RtmpH264ToPackets::new(capability.clock_rate, 96);
    let metadata = StreamMetadata {
        capability: crate::webrtc::from_codec_capability(capability),
    };

    let mut source_sequence_id = 0;
    let mut source_timestamp_reference = 0;
    let peer_id = identity.peer_id;
    let mut state = State::Waiting;
    let mut seek_type = StreamSeekType::Reset;
    let mut stream_offset = StreamOffset {
        timestamp_reference: 0,
        sequence_id: 0,
    };

    let (mut session, mut current_results) = ServerSession::new(ServerSessionConfig::new())?;
    current_results.extend(session.handle_input(&start_handshake(&mut stream).await?)?);

    let (read_stream, stream) = tokio::io::split(stream);
    let mut stream = BufWriter::new(stream);
    let (reader_sender, mut reader_receiver) = mpsc::channel::<Bytes>(10);
    let _reader_daemon_handle =
        spawn_and_log_error(connection_reader_daemon(read_stream, reader_sender));


    let mut writer: Option<crate::media::RtmpH264ToMp4<std::io::Cursor<Vec<u8>>>> = None;

    loop {
        let mut next_results: Vec<ServerSessionResult> = Vec::new();
        let mut packets: Vec<rml_rtmp::chunk_io::Packet> = Vec::new();
        current_results.retain(|result| match result {
            ServerSessionResult::OutboundResponse(p) => {
                packets.push(clone(p));
                false
            }
            _ => true,
        });

        if let State::Playing {
            app_name: _,
            stream_key: _,
            stream_id,
            publisher,
        } = &state
        {
            match publisher.get_data(peer_id, seek_type).await {
                Ok(Ok(StreamingResponse::Data(out))) if !out.is_empty() => {
                    let out: Vec<Result<Packet, ServerSessionError>> = out
                        .into_iter()
                        .map(|r| {
                            stream_offset = max(r.streaming_data.source_offset, stream_offset);
                            let frame_type = RTMPFrameType::classify(&r.streaming_data.data);
                            log::trace!("Received {frame_type:?}");
                            match r.streaming_data.data_type {
                                StreamingDataType::RtmpAudio(m) => {
                                    session.send_audio_data(
                                        *stream_id,
                                        r.streaming_data.data.into(),
                                        RtmpTimestamp { value: m.timestamp },
                                        false, // FIXME: fill this?
                                    )
                                }
                                StreamingDataType::RtmpVideo(m) => {
                                    session.send_video_data(
                                        *stream_id,
                                        r.streaming_data.data.into(),
                                        RtmpTimestamp { value: m.timestamp },
                                        false, // FIXME: fill this?
                                    )
                                }
                                StreamingDataType::WebRtcRtpPacket => todo!(),
                            }
                        })
                        .collect();
                    seek_type = StreamSeekType::Offset(stream_offset);

                    for new_packet in out {
                        match new_packet {
                            Ok(new_packet) => packets.push(new_packet),
                            Err(e) => log::error!("Error: {}", e),
                        };
                    }
                }
                Ok(Ok(StreamingResponse::Data(_))) => {}
                Ok(Ok(StreamingResponse::MaxUploadRateReached)) => {
                    log::error!("Received StreamingResponse::MaxUploadRateReached");
                }
                Ok(Ok(StreamingResponse::TooMuchFlood)) => {
                    log::error!("Received StreamingResponse::TooMuchFlood");
                }
                Ok(Err(e)) => anyhow::bail!("{}", e),
                Err(e) => return Err(e),
            }
        }

        if !packets.is_empty() {
            log::debug!("Writing {} packets to RTMP endpoint", packets.len());
            for p in &packets {
                stream.write_all(&p.bytes).await?;
            }
            stream.flush().await?;
        }

        if packets.is_empty() && current_results.is_empty() {
            log::debug!("Sleeping because no RTMP activity for now");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        log::debug!(
            "Processing {} events from RTMP endpoint",
            current_results.len()
        );
        for result in current_results.drain(..) {
            match result {
                ServerSessionResult::OutboundResponse(_) => {
                    unreachable!("All OutboundResponses have been removed from results");
                }
                ServerSessionResult::RaisedEvent(e) => match e {
                    ServerSessionEvent::ClientChunkSizeChanged { new_chunk_size } => {
                        log::warn!(
                            "Not implemented ServerSessionEvent::ClientChunkSizeChanged: {}",
                            new_chunk_size
                        );
                    }
                    ServerSessionEvent::ConnectionRequested {
                        request_id,
                        app_name,
                    } => {
                        log::info!(
                            "ServerSessionEvent::ConnectionRequested {} {}",
                            request_id,
                            app_name
                        );
                        matches!(state, State::Waiting);

                        let more_results = session.accept_request(request_id)?;
                        next_results.extend(more_results);
                        state = State::Connected { app_name };
                    }
                    ServerSessionEvent::ReleaseStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                    } => log::warn!(
                        "Not implemented ServerSessionEvent::ReleaseStreamRequested: {} {} {}",
                        request_id,
                        app_name,
                        stream_key
                    ),
                    ServerSessionEvent::PublishStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                        mode,
                    } => {
                        log::info!(
                            "ServerSessionEvent::PublishStreamRequested {} {} {} {:?}",
                            request_id,
                            app_name,
                            stream_key,
                            mode
                        );
                        assert!(match state {
                            State::Connected {
                                app_name: _app_name,
                            } => _app_name == app_name,
                            _ => false,
                        });

                        let stream_key = match PeerId::from_str(&stream_key) {
                            Ok(p) => p,
                            Err(e) => {
                                reader_receiver.close();
                                stream.shutdown().await?;
                                // reader_daemon_handle.abort();
                                anyhow::bail!("Error converting stream key to peer id: {}", e);
                            }
                        };

                        if stream_key != peer_id {
                            reader_receiver.close();
                            stream.shutdown().await?;
                            // reader_daemon_handle.abort();
                            anyhow::bail!(
                                "We only accept publish for stream key: {peer_id} but received for: {stream_key}"
                            );
                        }

                        let streaming_key = StreamingKey {
                            video_key: bs58::decode(app_name.clone()).into_vec()?,
                            channel_key: stream_key,
                        };
                        let (sender, receiver) = oneshot::channel();
                        let record = StreamingRecord::new(
                            &identity.keypair,
                            &streaming_key,
                            SystemTime::now().into(),
                        )?;
                        if cm::modules::streaming::publish(
                            &shared_global_state,
                            &mut network,
                            PublishStreamInfo {
                                record: record.clone(),
                                sender,
                            },
                        )
                        .await
                        .is_err()
                        {
                            reader_receiver.close();
                            stream.shutdown().await?;
                            // reader_daemon_handle.abort();
                            anyhow::bail!("receiver died")
                        };

                        let publisher = match receiver.await {
                            Ok(Ok(p)) => p,
                            _ => {
                                reader_receiver.close();
                                stream.shutdown().await?;
                                // reader_daemon_handle.abort();
                                anyhow::bail!("Aborting because publish failed");
                            }
                        };

                        let new_results = session.accept_request(request_id)?;
                        next_results.extend(new_results);
                        source_timestamp_reference = chrono::offset::Utc::now().timestamp() as u32;


                        // let w = writer.as_mut().unwrap();
                        // let video_track = mp4::TrackConfig {
                        //     language: "und".to_owned(),
                        //     timescale: 1000,
                        //     track_type: mp4::TrackType::Video,
                        //     media_conf: mp4::MediaConfig::AvcConfig(mp4::AvcConfig {
                        //         width: 1920,
                        //         height: 1080,
                        //         ..Default::default()
                        //     }),
                        // };
                        // w.add_track(&video_track)?;
                        // let audio_track = mp4::TrackConfig {
                        //     language: "und".to_owned(),
                        //     timescale: 1000,
                        //     track_type: mp4::TrackType::Audio,
                        //     media_conf: mp4::MediaConfig::AacConfig(mp4::AacConfig {
                        //         bitrate: 0,
                        //         chan_conf: mp4::ChannelConfig::Stereo,
                        //         freq_index: mp4::SampleFreqIndex::Freq44100,
                        //         profile: mp4::AudioObjectType::AacLowComplexity,
                        //     }),
                        // };
                        // w.add_track(&audio_track)?;

                        state = State::Publishing {
                            app_name,
                            stream_key,
                            publisher,
                            record,
                        };
                    }
                    ServerSessionEvent::PublishStreamFinished {
                        app_name,
                        stream_key,
                    } => {
                        log::info!(
                            "ServerSessionEvent::PublishStreamFinished {} {}",
                            app_name,
                            stream_key
                        );

                        let out_name = "/tmp/output.mp4";
                        let mut output = File::create(out_name).await?;
                        let w = writer.take().unwrap();
                        let a = w.finish()?.into_inner();
                        output.write_all(&a).await?;
                        output.flush().await?;
                        log::info!("Finished writing to {out_name}");

                        return Ok(());
                    }
                    ServerSessionEvent::StreamMetadataChanged {
                        app_name,
                        stream_key,
                        metadata,
                    } => {
                        log::info!(
                            "ServerSessionEvent::StreamMetadataChanged {} {} {:?}",
                            app_name,
                            stream_key,
                            metadata
                        );
                        // StreamMetadata { video_width: Some(1920), video_height: Some(1080), video_codec: Some("avc1"), video_frame_rate: Some(30.0), video_bitrate_kbps: Some(5000), audio_codec: Some("mp4a"), audio_bitrate_kbps: Some(160), audio_sample_rate: Some(44100), audio_channels: Some(2), audio_is_stereo: Some(true), encoder: Some("obs-output module (libobs ...)") }

                        if let (Some(fps), Some(audio_bitrate)) =
                            (metadata.video_frame_rate, metadata.audio_bitrate_kbps)
                        {
                            if writer.is_some() {
                                anyhow::bail!(
                                    "Stream is already initialized, this is unsupported right know"
                                );
                            }
                            writer = {
                                let config = mp4::Mp4Config {
                                    major_brand: str::parse("isom").unwrap(),
                                    minor_version: 512,
                                    compatible_brands: vec![str::parse("isom").unwrap()],
                                    timescale: 1000,
                                };
                                let w = mp4::Mp4Writer::write_start(
                                    std::io::Cursor::new(Vec::<u8>::new()),
                                    &config,
                                )?;
                                let w =
                                    crate::media::RtmpH264ToMp4::new(90000, fps, audio_bitrate, w);
                                Some(w)
                            };
                        } else {
                            anyhow::bail!("No fps information on metadata");
                        }
                    }
                    ServerSessionEvent::AudioDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => {
                        if data.len() <= 2 {
                            anyhow::bail!(
                                "ServerSessionEvent::AudioDataReceived invalid data {:?}",
                                data
                            );
                        }
                        let frame_type = RTMPFrameType::classify(&data);
                        log::debug!(
                            "ServerSessionEvent::AudioDataReceived {} {} {:#04x} {:#04x} {:?}",
                            &app_name,
                            &stream_key,
                            data[0],
                            data[1],
                            frame_type
                        );
                        match frame_type {
                            RTMPFrameType::Invalid | RTMPFrameType::Other => {
                                reader_receiver.close();
                                stream.shutdown().await?;
                                // reader_daemon_handle.abort();
                                anyhow::bail!(
                                    "Unrecognized audio frame type: {frame_type:?} {:#04x} {:#04x} ",
                                    data[0],
                                    data[1],
                                )
                            }
                            _ => {}
                        }

                        let w = writer.as_mut().unwrap();
                        w.process_audio(data, timestamp.value)?;

                        // let (publisher, record) = match &state {
                        //     State::Publishing {
                        //         app_name: _app_name,
                        //         stream_key: _stream_key,
                        //         publisher,
                        //         record,
                        //     } if *_app_name == app_name
                        //         && _stream_key.to_string() == stream_key =>
                        //     {
                        //         (publisher, record)
                        //     }
                        //     _ => {
                        //         anyhow::bail!("Invalid state for {} {}", app_name, stream_key)
                        //     }
                        // };
                        // let stream_data = vec![SignedStreamingData::from(
                        //     StreamingData {
                        //         data: data.to_vec(),
                        //         source_offset: StreamOffset {
                        //             sequence_id: source_sequence_id,
                        //             timestamp_reference: source_timestamp_reference,
                        //         },
                        //         data_type: StreamingDataType::RtmpAudio,
                        //     },
                        //     &identity.keypair,
                        //     record,
                        // )?];
                        // if publisher.feed_data(stream_data).await.is_err() {
                        //     reader_receiver.close();
                        //     stream.shutdown().await?;
                        //     // reader_daemon_handle.abort();
                        //     anyhow::bail!("Aborting because sending data failed");
                        // };
                        source_sequence_id += 1;
                    }
                    ServerSessionEvent::VideoDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => {
                        if data.len() <= 2 {
                            anyhow::bail!(
                                "ServerSessionEvent::VideoDataReceived invalid data {:?}",
                                data
                            );
                        }
                        let frame_type = RTMPFrameType::classify(&data);
                        log::debug!(
                            "ServerSessionEvent::VideoDataReceived {} {} {:#04x} {:#04x} {:?}",
                            &app_name,
                            &stream_key,
                            data[0],
                            data[1],
                            frame_type
                        );
                        match frame_type {
                            RTMPFrameType::Invalid | RTMPFrameType::Other => {
                                reader_receiver.close();
                                stream.shutdown().await?;
                                // reader_daemon_handle.abort();
                                anyhow::bail!(
                                    "Unrecognized video frame type: {frame_type:?} {:#04x} {:#04x} ",
                                    data[0],
                                    data[1],
                                )
                            }
                            _ => {}
                        }
                        let (publisher, record) = match &state {
                            State::Publishing {
                                app_name: _app_name,
                                stream_key: _stream_key,
                                publisher,
                                record,
                            } if *_app_name == app_name
                                && _stream_key.to_string() == stream_key =>
                            {
                                (publisher, record)
                            }
                            _ => {
                                anyhow::bail!("Invalid state for {} {}", app_name, stream_key);
                            }
                        };


                        // for packet in h264_processor.process(&data, timestamp.value)? {
                        //     let serialized = serialize_packet(&packet)?;
                        //     udp_socket.send(&serialized).await?;
                        // }

                        let stream_track = IntegerStreamTrack {
                            track_id: 0,
                            stream_id: 0,
                        };

                        let streaming_data = VerifiedSignedStreamingData::from(
                            StreamingData {
                                data: data.to_vec(),
                                source_offset: StreamOffset {
                                    sequence_id: source_sequence_id,
                                    timestamp_reference: source_timestamp_reference,
                                },
                                data_type: StreamingDataType::RtmpVideo(RtmpMetadata {
                                    timestamp: timestamp.value,
                                }),
                                metadata: Some(metadata.clone()), // FIXME: include only once
                                stream_track: stream_track.clone(),
                            },
                            &identity.keypair,
                            record,
                        )?;
                        publisher.feed_data(vec![streaming_data]).await?;
                        source_sequence_id += 1;


                        let w = writer.as_mut().unwrap();
                        w.process_video(data, timestamp.value)?;


                        // let sample = mp4::Mp4Sample { start_time: todo!(), duration: todo!(), rendering_offset: todo!(), is_sync: todo!(), bytes: todo!() };
                        // w.write_sample(0, );

                        // let rtmp_data = vec![SignedStreamingData::from(
                        //     StreamingData {
                        //         data,
                        //         timestamp: timestamp.value,
                        //         source_offset: StreamOffset {
                        //             sequence_id: source_sequence_id,
                        //             timestamp_reference: source_timestamp_reference,
                        //         },
                        //         data_type: StreamingDataType::RtmpVideo,
                        //     },
                        //     &identity.keypair,
                        //     record,
                        // )?];
                        // if publisher.feed_data(rtmp_data).await.is_err() {
                        //     reader_receiver.close();
                        //     stream.shutdown().await?;
                        //     // reader_daemon_handle.abort();
                        //     anyhow::bail!("Aborting because sending data failed");
                        // };
                        source_sequence_id += 1;
                    }
                    ServerSessionEvent::UnhandleableAmf0Command {
                        command_name,
                        transaction_id: _,
                        command_object: _,
                        additional_values: _,
                    } => log::debug!(
                        "ServerSessionEvent::UnhandleableAmf0Command {}",
                        command_name
                    ),
                    ServerSessionEvent::PlayStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                        start_at,
                        duration,
                        reset,
                        stream_id,
                    } => {
                        log::info!(
                            "ServerSessionEvent::PlayStreamRequested {} {} {} {:?} {:?} {} {}",
                            request_id,
                            app_name,
                            stream_key,
                            start_at,
                            duration,
                            reset,
                            stream_id
                        );
                        assert!(match state {
                            State::Connected {
                                app_name: _app_name,
                            } => _app_name == app_name,
                            _ => false,
                        });
                        let stream_key = match PeerId::from_str(&stream_key) {
                            Ok(p) => p,
                            Err(e) => {
                                anyhow::bail!("Error converting stream key to peer id: {}", e);
                            }
                        };
                        let streaming_key = StreamingKey {
                            video_key: app_name.clone().as_bytes().to_owned(),
                            channel_key: stream_key,
                        };
                        let (sender, receiver) = oneshot::channel();
                        cm::modules::streaming::play(
                            identity.clone(),
                            &shared_global_state,
                            &mut network,
                            PlayStreamInfo {
                                streaming_key,
                                sender,
                            },
                        )
                        .await?;

                        let publisher = receiver.await??;

                        let new_results = session.accept_request(request_id)?;
                        next_results.extend(new_results);

                        state = State::Playing {
                            app_name,
                            stream_key,
                            stream_id,
                            publisher,
                        };
                    }
                    ServerSessionEvent::PlayStreamFinished {
                        app_name,
                        stream_key,
                    } => log::info!(
                        "ServerSessionEvent::PlayStreamFinished {} {}",
                        app_name,
                        stream_key
                    ),
                    ServerSessionEvent::AcknowledgementReceived { bytes_received } => log::info!(
                        "ServerSessionEvent::AcknowledgementReceived {} bytes",
                        bytes_received
                    ),
                    ServerSessionEvent::PingResponseReceived { timestamp } => {
                        log::info!("ServerSessionEvent::PingResponseReceived {:?}", timestamp)
                    }
                },
                ServerSessionResult::UnhandleableMessageReceived(m) => {
                    log::warn!(":ServerSessionResult::UnhandleableMessageReceived {:?}", m);
                }
            }
        }
        current_results.extend(next_results);
        if current_results.is_empty() {
            loop {
                tokio::task::yield_now().await;
                match reader_receiver.try_recv() {
                    Ok(b) => {
                        log::trace!("Read {} bytes from RTMP endpoint", b.len());
                        current_results.extend(session.handle_input(&b)?);
                    }
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        let out_name = "/tmp/output.mp4";
                        let mut output = File::create(out_name).await?;
                        let w = writer.take().unwrap();
                        let a = w.finish()?.into_inner();
                        output.write_all(&a).await?;
                        output.flush().await?;
                        log::info!("Finished writing to {out_name}");

                        log::info!("EOF reading RTMP endpoint, exiting");
                        return Ok(());
                    }
                };
            }
        } else {
            log::debug!(
                "There are {} results still to be processed, skipping stream read",
                current_results.len()
            );
        }
    }
}

pub(crate) async fn start_handshake(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut handshake = Handshake::new(PeerType::Server);
    let server_p0_and_1 = handshake.generate_outbound_p0_and_p1()?;
    stream.write_all(&server_p0_and_1).await?;
    let mut buffer = [0; 4096];
    loop {
        let bytes_read = stream.read(&mut buffer).await?;
        log::debug!("start_handshake read returned {} bytes", bytes_read);

        if bytes_read == 0 {
            return Ok(Default::default());
        }

        match handshake.process_bytes(&buffer[0..bytes_read])? {
            HandshakeProcessResult::InProgress { response_bytes } => {
                stream.write_all(&response_bytes).await?;
            }

            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                stream.write_all(&response_bytes).await?;
                return Ok(remaining_bytes);
            }
        }
    }
}

async fn connection_reader_daemon(
    stream: ReadHalf<TcpStream>,
    sender: mpsc::Sender<Bytes>,
) -> anyhow::Result<()> {
    let mut stream = BufReader::new(stream);
    let mut buffer = BytesMut::with_capacity(4096);
    log::debug!("Initializing connection_reader_daemon");
    loop {
        let bytes_read = stream.read_buf(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        let bytes = buffer.split_off(bytes_read);
        match sender.send(buffer.freeze()).await {
            Ok(_) => {}
            Err(e) => {
                log::warn!("Error sending read bytes: {}", e);
                break;
            }
        }

        buffer = bytes;
    }

    log::info!("Reader disconnected");
    Ok(())
}

fn _serialize_packet(p: &webrtc::rtp::packet::Packet) -> Result<Vec<u8>, anyhow::Error> {
    let expected_size = webrtc_util::MarshalSize::marshal_size(p);
    let mut data = vec![0; expected_size];
    let actual_size = webrtc_util::Marshal::marshal_to(p, &mut data)?;
    if expected_size != actual_size {
        anyhow::bail!("Wrong marshal size: expected {expected_size} but got {actual_size}");
    }
    Ok(data)
}


fn _from_to_streaming_data_packets(
    StreamOffset {
        mut sequence_id,
        timestamp_reference,
    }: StreamOffset,
    packets: Vec<webrtc::rtp::packet::Packet>,
    metadata: &StreamMetadata,
    stream_track: &IntegerStreamTrack,
    identity: &NodeIdentity,
    record: &StreamingRecord,
) -> (StreamOffset, Vec<VerifiedSignedStreamingData>) {
    let packets = packets
        .iter()
        .map(|data| {
            let r = VerifiedSignedStreamingData::from(
                StreamingData {
                    data: _serialize_packet(data)?,
                    source_offset: StreamOffset {
                        sequence_id,
                        timestamp_reference,
                    },
                    data_type: StreamingDataType::WebRtcRtpPacket,
                    metadata: Some(metadata.clone()), // FIXME: include only once
                    stream_track: stream_track.clone(),
                },
                &identity.keypair,
                record,
            );
            sequence_id += 1;
            r
        })
        .inspect(|r| {
            if let Err(ref e) = *r {
                log::warn!("SignedStreamingData creation error: {e}");
            }
        })
        .filter_map(Result::ok)
        .collect();

    (
        StreamOffset {
            sequence_id,
            timestamp_reference,
        },
        packets,
    )
}

#[allow(clippy::large_enum_variant)]
pub enum State {
    Waiting,
    Connected {
        app_name: String,
    },
    Publishing {
        app_name: String,
        stream_key: PeerId,
        publisher: StreamPublisherClient,
        record: StreamingRecord,
    },
    Playing {
        app_name: String,
        stream_key: PeerId,
        stream_id: u32,
        publisher: StreamPublisherClient,
    },
}
