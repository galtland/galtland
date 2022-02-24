// SPDX-License-Identifier: AGPL-3.0-only

use std::cmp::max;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use libp2p::bytes::BytesMut;
use libp2p::PeerId;
use rml_rtmp::chunk_io::Packet;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionError, ServerSessionEvent, ServerSessionResult,
};
use rml_rtmp::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;

use super::cm::rtmp::rtmp_publisher::RtmpPublisherClient;
use crate::daemons::cm::rtmp::handlers::{PlayRTMPStreamInfo, PublishRTMPStreamInfo};
use crate::daemons::cm::ClientCommand;
use crate::protocols::kademlia_record::RtmpStreamingRecord;
use crate::protocols::rtmp_streaming::{
    RTMPDataSeekType, RTMPFrameType, RtmpData, RtmpDataType, RtmpStreamingKey,
    RtmpStreamingResponse, SignedRtmpData, StreamOffset,
};
use crate::protocols::NodeIdentity;
use crate::utils::{self, spawn_and_log_error};

pub async fn accept_loop(
    addr: SocketAddr,
    identity: NodeIdentity,
    commands: Sender<ClientCommand>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    log::info!("RTMP Server is listening to {}", addr);
    loop {
        log::debug!("Looping accept loop");
        match listener.accept().await {
            Ok((stream, address)) => {
                log::info!("New connection: {}", address);
                let identity = identity.clone();
                let commands = commands.clone();
                spawn_and_log_error(connection_loop(stream, identity, commands));
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
    commands: Sender<ClientCommand>,
) -> anyhow::Result<()> {
    let mut source_sequence_id = 0;
    let mut source_timestamp_reference = 0;
    let peer_id = identity.peer_id;
    let mut state = State::Waiting;
    let mut seek_type = RTMPDataSeekType::Reset;
    let mut stream_offset = StreamOffset {
        timestamp_reference: 0,
        sequence_id: 0,
    };

    let (mut session, mut current_results) = ServerSession::new(ServerSessionConfig::new())?;
    current_results.extend(session.handle_input(&start_handshake(&mut stream).await?)?);

    let (read_stream, stream) = tokio::io::split(stream);
    let mut stream = BufWriter::new(stream);
    let (reader_sender, mut reader_receiver) = mpsc::channel::<Bytes>(10);
    let reader_daemon_handle =
        spawn_and_log_error(connection_reader_daemon(read_stream, reader_sender));

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
                Ok(Ok(RtmpStreamingResponse::Data(out))) if !out.is_empty() => {
                    let out: Vec<Result<Packet, ServerSessionError>> = out
                        .into_iter()
                        .map(|r| {
                            stream_offset = max(r.rtmp_data.source_offset, stream_offset);
                            let frame_type = RTMPFrameType::frame_type(&r.rtmp_data.data);
                            log::trace!("Received {frame_type:?}");
                            match r.rtmp_data.data_type {
                                RtmpDataType::Audio => {
                                    session.send_audio_data(
                                        *stream_id,
                                        r.rtmp_data.data,
                                        RtmpTimestamp {
                                            value: r.rtmp_data.rtmp_timestamp,
                                        },
                                        false, // FIXME: fill this?
                                    )
                                }
                                RtmpDataType::Video => {
                                    session.send_video_data(
                                        *stream_id,
                                        r.rtmp_data.data,
                                        RtmpTimestamp {
                                            value: r.rtmp_data.rtmp_timestamp,
                                        },
                                        false, // FIXME: fill this?
                                    )
                                }
                            }
                        })
                        .collect();
                    seek_type = RTMPDataSeekType::Offset(stream_offset);

                    for new_packet in out {
                        match new_packet {
                            Ok(new_packet) => packets.push(new_packet),
                            Err(e) => log::error!("Error: {}", e),
                        };
                    }
                }
                Ok(Ok(RtmpStreamingResponse::Data(_))) => {}
                Ok(Ok(RtmpStreamingResponse::MaxUploadRateReached)) => {
                    log::error!("Received RTMPStreamingResponse::MaxUploadRateReached");
                }
                Ok(Ok(RtmpStreamingResponse::TooMuchFlood)) => {
                    log::error!("Received RTMPStreamingResponse::TooMuchFlood");
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
                                reader_daemon_handle.abort();
                                anyhow::bail!("Error converting stream key to peer id: {}", e);
                            }
                        };

                        if stream_key != peer_id {
                            reader_receiver.close();
                            stream.shutdown().await?;
                            reader_daemon_handle.abort();
                            anyhow::bail!(
                                "We only accept publish for stream key: {peer_id} but received for: {stream_key}"
                            );
                        }

                        let streaming_key = RtmpStreamingKey {
                            app_name: app_name.clone(),
                            stream_key,
                        };
                        let (sender, receiver) = oneshot::channel();
                        let record = RtmpStreamingRecord::new(
                            &identity.keypair,
                            &streaming_key,
                            SystemTime::now().into(),
                        )?;
                        if commands
                            .send(ClientCommand::PublishRTMPStream(PublishRTMPStreamInfo {
                                record: record.clone(),
                                sender,
                            }))
                            .await
                            .is_err()
                        {
                            reader_receiver.close();
                            stream.shutdown().await?;
                            reader_daemon_handle.abort();
                            anyhow::bail!("receiver died")
                        };

                        let publisher = match receiver.await {
                            Ok(Ok(p)) => p,
                            _ => {
                                reader_receiver.close();
                                stream.shutdown().await?;
                                reader_daemon_handle.abort();
                                anyhow::bail!("Aborting because publish failed");
                            }
                        };

                        let new_results = session.accept_request(request_id)?;
                        next_results.extend(new_results);
                        source_timestamp_reference = chrono::offset::Utc::now().timestamp() as u32;
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
                    } => log::info!(
                        "ServerSessionEvent::PublishStreamFinished {} {}",
                        app_name,
                        stream_key
                    ),
                    ServerSessionEvent::StreamMetadataChanged {
                        app_name,
                        stream_key,
                        metadata,
                    } => log::info!(
                        "ServerSessionEvent::StreamMetadataChanged {} {} {:?}",
                        app_name,
                        stream_key,
                        metadata
                    ),
                    ServerSessionEvent::AudioDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => {
                        log::trace!(
                            "ServerSessionEvent::AudioDataReceived {} {}",
                            app_name,
                            stream_key
                        );
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
                                anyhow::bail!("Invalid state for {} {}", app_name, stream_key)
                            }
                        };
                        let rtmp_data = vec![SignedRtmpData::from(
                            RtmpData {
                                data,
                                rtmp_timestamp: timestamp.value,
                                source_offset: StreamOffset {
                                    sequence_id: source_sequence_id,
                                    timestamp_reference: source_timestamp_reference,
                                },
                                data_type: RtmpDataType::Audio,
                            },
                            &identity.keypair,
                            record,
                        )?];
                        if publisher.feed_data(rtmp_data).await.is_err() {
                            reader_receiver.close();
                            stream.shutdown().await?;
                            reader_daemon_handle.abort();
                            anyhow::bail!("Aborting because sending data failed");
                        };
                        source_sequence_id += 1;
                    }
                    ServerSessionEvent::VideoDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => {
                        log::trace!(
                            "ServerSessionEvent::VideoDataReceived {} {}",
                            &app_name,
                            &stream_key
                        );
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
                        let rtmp_data = vec![SignedRtmpData::from(
                            RtmpData {
                                data,
                                rtmp_timestamp: timestamp.value,
                                source_offset: StreamOffset {
                                    sequence_id: source_sequence_id,
                                    timestamp_reference: source_timestamp_reference,
                                },
                                data_type: RtmpDataType::Video,
                            },
                            &identity.keypair,
                            record,
                        )?];
                        if publisher.feed_data(rtmp_data).await.is_err() {
                            reader_receiver.close();
                            stream.shutdown().await?;
                            reader_daemon_handle.abort();
                            anyhow::bail!("Aborting because sending data failed");
                        };
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
                        let streaming_key = RtmpStreamingKey {
                            app_name: app_name.clone(),
                            stream_key,
                        };
                        let (sender, receiver) = oneshot::channel();
                        commands
                            .send(ClientCommand::PlayRTMPStream(PlayRTMPStreamInfo {
                                streaming_key,
                                sender,
                            }))
                            .await
                            .map_err(utils::send_error)?;

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

pub async fn start_handshake(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
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

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum State {
    Waiting,
    Connected {
        app_name: String,
    },
    Publishing {
        app_name: String,
        stream_key: PeerId,
        publisher: RtmpPublisherClient,
        record: RtmpStreamingRecord,
    },
    Playing {
        app_name: String,
        stream_key: PeerId,
        stream_id: u32,
        publisher: RtmpPublisherClient,
    },
}
