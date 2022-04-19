use anyhow::Context;
use galtcore::daemons::cm::{self, ClientCommand};
use galtcore::protocols::kademlia_record::StreamingRecord;
use galtcore::protocols::media_streaming::{
    SignedStreamingData, StreamMetadata, StreamOffset, StreamTrack, StreamingData,
    StreamingDataType, StreamingKey,
};
use galtcore::protocols::NodeIdentity;
use galtcore::utils;
use instant::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use webrtc::api::media_engine::{MIME_TYPE_H264, MIME_TYPE_OPUS, MIME_TYPE_VP8};
use webrtc::media::io::h264_reader::H264Reader;
use webrtc::media::io::ogg_reader::OggReader;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::rtp_transceiver::RTCPFeedback;
use webrtc_util::Unmarshal;

const RTP_OUTBOUND_MTU: usize = 1200;
const OGG_PAGE_DURATION: Duration = Duration::from_millis(20);


// pub async fn publish(
//     video_file: String,
//     audio_file: String,
//     streaming_key: StreamingKey,
//     commands: mpsc::Sender<ClientCommand>,
//     identity: NodeIdentity,
// ) -> anyhow::Result<()> {
//     let record = StreamingRecord::new(
//         &identity.keypair,
//         &streaming_key,
//         instant::SystemTime::now().into(),
//     )?;

//     let (streaming_data_sender, mut streaming_data_receiver) = mpsc::unbounded_channel();
//     let video_handle = tokio::task::spawn_blocking({
//         let record = record.clone();
//         let identity = identity.clone();
//         let streaming_data_sender = streaming_data_sender.clone();
//         let capability = RTCRtpCodecCapability {
//             mime_type: MIME_TYPE_H264.to_owned(),
//             clock_rate: 90000,
//             channels: 0,
//             sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f"
//                 .to_owned(),
//             rtcp_feedback: vec![
//                 RTCPFeedback {
//                     typ: "goog-remb".to_owned(),
//                     parameter: "".to_owned(),
//                 },
//                 RTCPFeedback {
//                     typ: "ccm".to_owned(),
//                     parameter: "fir".to_owned(),
//                 },
//                 RTCPFeedback {
//                     typ: "nack".to_owned(),
//                     parameter: "".to_owned(),
//                 },
//                 RTCPFeedback {
//                     typ: "nack".to_owned(),
//                     parameter: "pli".to_owned(),
//                 },
//             ],
//         };
//         let metadata = StreamMetadata {
//             capability: super::from_codec_capability(capability),
//         };
//         let file = std::fs::File::open(&video_file)
//             .map_err(|e| anyhow::anyhow!("tried to open file {video_file} but got: {e:?}"))?;
//         let stream_track = StreamTrack {
//             stream_id: 0,
//             track_id: 0,
//         };
//         let clock_rate = metadata.capability.clock_rate;
//         let samples = clock_rate; // TODO: figure it out
//         let payloader = Box::new(webrtc::rtp::codecs::h264::H264Payloader::default());
//         let sequencer: Box<dyn webrtc::rtp::sequence::Sequencer + Send + Sync> =
//             Box::new(webrtc::rtp::sequence::new_random_sequencer());
//         let mut packetizer = webrtc::rtp::packetizer::new_packetizer(
//             RTP_OUTBOUND_MTU,
//             102, // payload_type for h264
//             0,   // TODO: check this
//             payloader,
//             sequencer.clone(),
//             clock_rate,
//         );
//         let mut ticker = tokio::time::interval(Duration::from_millis(33)); //FIXME: get from file
//         let reader = std::io::BufReader::new(file);
//         let mut h264 = H264Reader::new(reader);
//         move || {
//             loop {
//                 Handle::current().block_on(ticker.tick());
//                 let nal = match h264.next_nal() {
//                     Ok(nal) => nal,
//                     Err(e) => {
//                         log::info!("All video frames parsed and sent: {}", e);
//                         break;
//                     }
//                 };
//                 log::debug!(
//                     "PictureOrderCount={}, ForbiddenZeroBit={}, RefIdc={}, UnitType={}, data={}",
//                     nal.picture_order_count,
//                     nal.forbidden_zero_bit,
//                     nal.ref_idc,
//                     nal.unit_type,
//                     nal.data.len()
//                 );
//                 let payload = nal.data.freeze();
//                 let packets = Handle::current()
//                     .block_on(webrtc::rtp::packetizer::Packetizer::packetize(
//                         &mut packetizer,
//                         &payload,
//                         samples,
//                     ))
//                     .context("video packetize")?;
//                 let streaming_data =
//                     to_streaming_data(packets, &metadata, &stream_track, &identity, &record);
//                 streaming_data_sender
//                     .send(streaming_data)
//                     .context("receiver dropped sending video streaming data")?;
//             }
//             Ok::<(), anyhow::Error>(())
//         }
//     });

//     let audio_handle = tokio::task::spawn_blocking({
//         let record = record.clone();
//         let capability = RTCRtpCodecCapability {
//             mime_type: MIME_TYPE_OPUS.to_owned(),
//             clock_rate: 48000,
//             channels: 2,
//             sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
//             rtcp_feedback: vec![],
//         };
//         let metadata = StreamMetadata {
//             capability: super::from_codec_capability(capability),
//         };
//         let file = std::fs::File::open(&audio_file)
//             .map_err(|e| anyhow::anyhow!("tried to open file {audio_file} but got: {e:?}"))?;
//         let stream_track = StreamTrack {
//             stream_id: 0,
//             track_id: 1,
//         };
//         let clock_rate = metadata.capability.clock_rate;
//         let payloader = Box::new(webrtc::rtp::codecs::opus::OpusPayloader::default());
//         let sequencer: Box<dyn webrtc::rtp::sequence::Sequencer + Send + Sync> =
//             Box::new(webrtc::rtp::sequence::new_random_sequencer());
//         let mut packetizer = webrtc::rtp::packetizer::new_packetizer(
//             RTP_OUTBOUND_MTU,
//             111, // payload_type for opus
//             0,   // TODO: check this
//             payloader,
//             sequencer.clone(),
//             clock_rate,
//         );
//         let mut ticker = tokio::time::interval(OGG_PAGE_DURATION); //TODO: get from file?
//         let reader = std::io::BufReader::new(file);
//         let (mut ogg, _) = OggReader::new(reader, true)?;
//         move || {
//             let mut last_granule: u64 = 0;
//             while let Ok((page_data, page_header)) = ogg.parse_next_page() {
//                 // The amount of samples is the difference between the last and current timestamp
//                 let sample_count = page_header.granule_position - last_granule;
//                 last_granule = page_header.granule_position;
//                 let sample_duration = Duration::from_millis(sample_count * 1000 / 48000);
//                 let samples = (sample_duration.as_secs_f64() * clock_rate as f64) as u32; // TODO: double check this

//                 let payload = page_data.freeze();
//                 let packets = Handle::current()
//                     .block_on(webrtc::rtp::packetizer::Packetizer::packetize(
//                         &mut packetizer,
//                         &payload,
//                         samples,
//                     ))
//                     .context("audio packetize")?;
//                 let streaming_data =
//                     to_streaming_data(packets, &metadata, &stream_track, &identity, &record);
//                 streaming_data_sender
//                     .send(streaming_data)
//                     .context("receiver dropped sending audio streaming data")?;
//                 Handle::current().block_on(ticker.tick());
//             }
//             Ok::<(), anyhow::Error>(())
//         }
//     });

//     let (sender, receiver) = oneshot::channel();
//     commands
//         .send(ClientCommand::PublishStream(
//             cm::streaming::handlers::PublishStreamInfo {
//                 record: record.clone(),
//                 sender,
//             },
//         ))
//         .await
//         .map_err(utils::send_error)?;

//     let publisher = receiver
//         .await
//         .context("first publish receive error")?
//         .context("second publish receive error")?;

//     utils::spawn_and_log_error(async move {
//         let timestamp_reference = chrono::offset::Utc::now().timestamp() as u32;
//         let mut sequence_id = 0;
//         while let Some(streaming_data) = streaming_data_receiver.recv().await {
//             let streaming_data = streaming_data
//                 .into_iter()
//                 .map(|mut s| {
//                     s.streaming_data.source_offset = StreamOffset {
//                         sequence_id,
//                         timestamp_reference,
//                     };
//                     sequence_id += 1;
//                     s
//                 })
//                 .collect();
//             publisher.feed_data(streaming_data).await?;
//         }
//         Ok(())
//     });

//     video_handle.await??;
//     audio_handle.await??;
//     Ok(())
// }


// pub async fn publish2(
//     video_file: String,
//     streaming_key: StreamingKey,
//     commands: mpsc::Sender<ClientCommand>,
//     identity: NodeIdentity,
// ) -> anyhow::Result<()> {
//     let record = StreamingRecord::new(
//         &identity.keypair,
//         &streaming_key,
//         instant::SystemTime::now().into(),
//     )?;

//     let (streaming_data_sender, mut streaming_data_receiver) = mpsc::unbounded_channel();
//     let video_handle = tokio::task::spawn_blocking({
//         let record = record.clone();
//         let identity = identity.clone();
//         let streaming_data_sender = streaming_data_sender.clone();
//         let capability = RTCRtpCodecCapability {
//             mime_type: MIME_TYPE_H264.to_owned(),
//             clock_rate: 90000,
//             channels: 0,
//             sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f"
//                 .to_owned(),
//             rtcp_feedback: vec![
//                 RTCPFeedback {
//                     typ: "goog-remb".to_owned(),
//                     parameter: "".to_owned(),
//                 },
//                 RTCPFeedback {
//                     typ: "ccm".to_owned(),
//                     parameter: "fir".to_owned(),
//                 },
//                 RTCPFeedback {
//                     typ: "nack".to_owned(),
//                     parameter: "".to_owned(),
//                 },
//                 RTCPFeedback {
//                     typ: "nack".to_owned(),
//                     parameter: "pli".to_owned(),
//                 },
//             ],
//         };
//         let metadata = StreamMetadata {
//             capability: super::from_codec_capability(capability),
//         };
//         let file = std::fs::File::open(&video_file)
//             .map_err(|e| anyhow::anyhow!("tried to open file {video_file} but got: {e:?}"))?;
//         let stream_track = StreamTrack {
//             stream_id: 0,
//             track_id: 0,
//         };
//         let clock_rate = metadata.capability.clock_rate;
//         let samples = clock_rate; // TODO: figure it out
//         let payloader = Box::new(webrtc::rtp::codecs::h264::H264Payloader::default());
//         let sequencer: Box<dyn webrtc::rtp::sequence::Sequencer + Send + Sync> =
//             Box::new(webrtc::rtp::sequence::new_random_sequencer());
//         let mut packetizer = webrtc::rtp::packetizer::new_packetizer(
//             RTP_OUTBOUND_MTU,
//             102, // payload_type for h264
//             0,   // TODO: check this
//             payloader,
//             sequencer.clone(),
//             clock_rate,
//         );
//         let mut ticker = tokio::time::interval(Duration::from_millis(33)); //FIXME: get from file
//         let size = file.metadata()?.len();
//         let reader = std::io::BufReader::new(file);
//         let mp4 = mp4::Mp4Reader::read_header(reader, size)?;
//         let track_id = 1;
//         let adapter = crate::media::Mp4ReaderAdapter::new(mp4, track_id)?;
//         let mut h264 = H264Reader::new(adapter);
//         move || {
//             loop {
//                 Handle::current().block_on(ticker.tick());
//                 let nal = match h264.next_nal() {
//                     Ok(nal) => nal,
//                     Err(e) => {
//                         log::info!("All video frames parsed and sent: {}", e);
//                         break;
//                     }
//                 };
//                 log::debug!(
//                     "PictureOrderCount={}, ForbiddenZeroBit={}, RefIdc={}, UnitType={}, data={}",
//                     nal.picture_order_count,
//                     nal.forbidden_zero_bit,
//                     nal.ref_idc,
//                     nal.unit_type,
//                     nal.data.len()
//                 );
//                 let payload = nal.data.freeze();
//                 let packets = Handle::current()
//                     .block_on(webrtc::rtp::packetizer::Packetizer::packetize(
//                         &mut packetizer,
//                         &payload,
//                         samples,
//                     ))
//                     .context("video packetize")?;
//                 let streaming_data =
//                     to_streaming_data(packets, &metadata, &stream_track, &identity, &record);
//                 streaming_data_sender
//                     .send(streaming_data)
//                     .context("receiver dropped sending video streaming data")?;
//             }
//             Ok::<(), anyhow::Error>(())
//         }
//     });

//     // let audio_handle = tokio::task::spawn_blocking({
//     //     let record = record.clone();
//     //     let capability = RTCRtpCodecCapability {
//     //         mime_type: MIME_TYPE_OPUS.to_owned(),
//     //         clock_rate: 48000,
//     //         channels: 2,
//     //         sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
//     //         rtcp_feedback: vec![],
//     //     };
//     //     let metadata = StreamMetadata {
//     //         capability: super::from_codec_capability(capability),
//     //     };
//     //     let file = std::fs::File::open(&audio_file)
//     //         .map_err(|e| anyhow::anyhow!("tried to open file {audio_file} but got: {e:?}"))?;
//     //     let stream_track = StreamTrack {
//     //         stream_id: 0,
//     //         track_id: 1,
//     //     };
//     //     let clock_rate = metadata.capability.clock_rate;
//     //     let payloader = Box::new(webrtc::rtp::codecs::opus::OpusPayloader::default());
//     //     let sequencer: Box<dyn webrtc::rtp::sequence::Sequencer + Send + Sync> =
//     //         Box::new(webrtc::rtp::sequence::new_random_sequencer());
//     //     let mut packetizer = webrtc::rtp::packetizer::new_packetizer(
//     //         RTP_OUTBOUND_MTU,
//     //         111, // payload_type for opus
//     //         0,   // TODO: check this
//     //         payloader,
//     //         sequencer.clone(),
//     //         clock_rate,
//     //     );
//     //     let mut ticker = tokio::time::interval(OGG_PAGE_DURATION); //TODO: get from file?
//     //     let reader = std::io::BufReader::new(file);
//     //     let (mut ogg, _) = OggReader::new(reader, true)?;
//     //     move || {
//     //         let mut last_granule: u64 = 0;
//     //         while let Ok((page_data, page_header)) = ogg.parse_next_page() {
//     //             // The amount of samples is the difference between the last and current timestamp
//     //             let sample_count = page_header.granule_position - last_granule;
//     //             last_granule = page_header.granule_position;
//     //             let sample_duration = Duration::from_millis(sample_count * 1000 / 48000);
//     //             let samples = (sample_duration.as_secs_f64() * clock_rate as f64) as u32; // TODO: double check this

//     //             let payload = page_data.freeze();
//     //             let packets = Handle::current()
//     //                 .block_on(webrtc::rtp::packetizer::Packetizer::packetize(
//     //                     &mut packetizer,
//     //                     &payload,
//     //                     samples,
//     //                 ))
//     //                 .context("audio packetize")?;
//     //             let streaming_data =
//     //                 to_streaming_data(packets, &metadata, &stream_track, &identity, &record);
//     //             streaming_data_sender
//     //                 .send(streaming_data)
//     //                 .context("receiver dropped sending audio streaming data")?;
//     //             Handle::current().block_on(ticker.tick());
//     //         }
//     //         Ok::<(), anyhow::Error>(())
//     //     }
//     // });

//     let (sender, receiver) = oneshot::channel();
//     commands
//         .send(ClientCommand::PublishStream(
//             cm::streaming::handlers::PublishStreamInfo {
//                 record: record.clone(),
//                 sender,
//             },
//         ))
//         .await
//         .map_err(utils::send_error)?;

//     let publisher = receiver
//         .await
//         .context("first publish receive error")?
//         .context("second publish receive error")?;

//     utils::spawn_and_log_error(async move {
//         let timestamp_reference = chrono::offset::Utc::now().timestamp() as u32;
//         let mut sequence_id = 0;
//         while let Some(streaming_data) = streaming_data_receiver.recv().await {
//             let streaming_data = streaming_data
//                 .into_iter()
//                 .map(|mut s| {
//                     s.streaming_data.source_offset = StreamOffset {
//                         sequence_id,
//                         timestamp_reference,
//                     };
//                     sequence_id += 1;
//                     s
//                 })
//                 .collect();
//             publisher.feed_data(streaming_data).await?;
//         }
//         Ok(())
//     });

//     video_handle.await??;
//     // audio_handle.await??;
//     Ok(())
// }


pub async fn publish3(
    video_file: String,
    audio_file: String,
    streaming_key: StreamingKey,
    commands: mpsc::Sender<ClientCommand>,
    identity: NodeIdentity,
) -> anyhow::Result<()> {
    let record = StreamingRecord::new(
        &identity.keypair,
        &streaming_key,
        instant::SystemTime::now().into(),
    )?;
    /*
    v=0
    o=- 0 0 IN IP4 127.0.0.1
    s=Big Buck Bunny, Sunflower version
    c=IN IP4 127.0.0.1
    t=0 0
    a=tool:libavformat 58.29.100
    m=video 5000 RTP/AVP 96
    b=AS:1995
    a=rtpmap:96 H264/90000
    a=fmtp:96 packetization-mode=1; sprop-parameter-sets=Z2QAKqyyAPAET8uAiAAAAwAIAAADA8B4wZJA,aOvMsiw=; profile-level-id=64002A

    */
    let (streaming_data_sender, mut streaming_data_receiver) = mpsc::unbounded_channel();
    let video_handle = tokio::spawn({
        let record = record.clone();
        let identity = identity.clone();
        let streaming_data_sender = streaming_data_sender.clone();
        let video_rtcp_feedback = vec![
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
        ];
        let capability = RTCRtpCodecCapability {
            mime_type: MIME_TYPE_VP8.to_owned(),
            clock_rate: 90000,
            channels: 0,
            sdp_fmtp_line: "".to_owned(),
            rtcp_feedback: video_rtcp_feedback.clone(),
        };
        let metadata = StreamMetadata {
            capability: super::from_codec_capability(capability),
        };
        // let file = std::fs::File::open(&video_file)
        //     .map_err(|e| anyhow::anyhow!("tried to open file {video_file} but got: {e:?}"))?;
        let stream_track = StreamTrack {
            stream_id: 0,
            track_id: 0,
        };
        let clock_rate = metadata.capability.clock_rate;
        let samples = clock_rate; // TODO: figure it out
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:5000").await?;
        log::debug!("listening udp");
        let mut buf = [0u8; 2048];
        async move {
            loop {
                let (len, addr) = socket.recv_from(&mut buf).await?;
                log::debug!("received udp packet from {addr}: {len} bytes");
                // Handle::current().block_on(ticker.tick());
                // let nal = match h264.next_nal() {
                //     Ok(nal) => nal,
                //     Err(e) => {
                //         log::info!("All video frames parsed and sent: {}", e);
                //         break;
                //     }
                // };
                // log::debug!(
                //     "PictureOrderCount={}, ForbiddenZeroBit={}, RefIdc={}, UnitType={}, data={}",
                //     nal.picture_order_count,
                //     nal.forbidden_zero_bit,
                //     nal.ref_idc,
                //     nal.unit_type,
                //     nal.data.len()
                // );
                // let payload = nal.data.freeze();
                // let packets = Handle::current()
                //     .block_on(webrtc::rtp::packetizer::Packetizer::packetize(
                //         &mut packetizer,
                //         &payload,
                //         samples,
                //     ))
                //     .context("video packetize")?;
                let mut packet = &buf[0..len];
                let unmarshal = webrtc::rtp::packet::Packet::unmarshal(&mut packet);
                // log::info!("packet: {unmarshal:?}");
                let packets = vec![packet.to_owned()];
                let streaming_data =
                    to_streaming_data(packets, &metadata, &stream_track, &identity, &record);
                streaming_data_sender
                    .send(streaming_data)
                    .context("receiver dropped sending video streaming data")?;
            }
            Ok::<(), anyhow::Error>(())
        }
    });

    // let audio_handle = tokio::task::spawn_blocking({
    //     let record = record.clone();
    //     let capability = RTCRtpCodecCapability {
    //         mime_type: MIME_TYPE_OPUS.to_owned(),
    //         clock_rate: 48000,
    //         channels: 2,
    //         sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
    //         rtcp_feedback: vec![],
    //     };
    //     let metadata = StreamMetadata {
    //         capability: super::from_codec_capability(capability),
    //     };
    //     let file = std::fs::File::open(&audio_file)
    //         .map_err(|e| anyhow::anyhow!("tried to open file {audio_file} but got: {e:?}"))?;
    //     let stream_track = StreamTrack {
    //         stream_id: 0,
    //         track_id: 1,
    //     };
    //     let clock_rate = metadata.capability.clock_rate;
    //     let payloader = Box::new(webrtc::rtp::codecs::opus::OpusPayloader::default());
    //     let sequencer: Box<dyn webrtc::rtp::sequence::Sequencer + Send + Sync> =
    //         Box::new(webrtc::rtp::sequence::new_random_sequencer());
    //     let mut packetizer = webrtc::rtp::packetizer::new_packetizer(
    //         RTP_OUTBOUND_MTU,
    //         111, // payload_type for opus
    //         0,   // TODO: check this
    //         payloader,
    //         sequencer.clone(),
    //         clock_rate,
    //     );
    //     let mut ticker = tokio::time::interval(OGG_PAGE_DURATION); //TODO: get from file?
    //     let reader = std::io::BufReader::new(file);
    //     let (mut ogg, _) = OggReader::new(reader, true)?;
    //     move || {
    //         let mut last_granule: u64 = 0;
    //         while let Ok((page_data, page_header)) = ogg.parse_next_page() {
    //             // The amount of samples is the difference between the last and current timestamp
    //             let sample_count = page_header.granule_position - last_granule;
    //             last_granule = page_header.granule_position;
    //             let sample_duration = Duration::from_millis(sample_count * 1000 / 48000);
    //             let samples = (sample_duration.as_secs_f64() * clock_rate as f64) as u32; // TODO: double check this

    //             let payload = page_data.freeze();
    //             let packets = Handle::current()
    //                 .block_on(webrtc::rtp::packetizer::Packetizer::packetize(
    //                     &mut packetizer,
    //                     &payload,
    //                     samples,
    //                 ))
    //                 .context("audio packetize")?;
    //             let streaming_data =
    //                 to_streaming_data(packets, &metadata, &stream_track, &identity, &record);
    //             streaming_data_sender
    //                 .send(streaming_data)
    //                 .context("receiver dropped sending audio streaming data")?;
    //             Handle::current().block_on(ticker.tick());
    //         }
    //         Ok::<(), anyhow::Error>(())
    //     }
    // });

    let (sender, receiver) = oneshot::channel();
    commands
        .send(ClientCommand::PublishStream(
            cm::streaming::handlers::PublishStreamInfo {
                record: record.clone(),
                sender,
            },
        ))
        .await
        .map_err(utils::send_error)?;

    let publisher = receiver
        .await
        .context("first publish receive error")?
        .context("second publish receive error")?;

    utils::spawn_and_log_error(async move {
        let timestamp_reference = chrono::offset::Utc::now().timestamp() as u32;
        let mut sequence_id = 0;
        while let Some(streaming_data) = streaming_data_receiver.recv().await {
            let streaming_data = streaming_data
                .into_iter()
                .map(|mut s| {
                    s.streaming_data.source_offset = StreamOffset {
                        sequence_id,
                        timestamp_reference,
                    };
                    sequence_id += 1;
                    s
                })
                .collect();
            publisher.feed_data(streaming_data).await?;
        }
        Ok(())
    });

    video_handle.await??;
    // audio_handle.await??;
    Ok(())
}


fn to_streaming_data(
    packets: Vec<Vec<u8>>,
    metadata: &StreamMetadata,
    stream_track: &StreamTrack,
    identity: &NodeIdentity,
    record: &StreamingRecord,
) -> Vec<SignedStreamingData> {
    packets
        .into_iter()
        .map(|data| {
            SignedStreamingData::from(
                StreamingData {
                    data,
                    source_offset: Default::default(), // will be properly filled latter
                    data_type: StreamingDataType::WebRtcRtpPacket,
                    metadata: Some(metadata.clone()), // FIXME: include only once
                    stream_track: stream_track.clone(),
                },
                &identity.keypair,
                record,
            )
        })
        .inspect(|r| {
            if let Err(ref e) = *r {
                log::warn!("SignedStreamingData creation error: {e}");
            }
        })
        .filter_map(Result::ok)
        .collect()
}

fn serialize_packet(p: &webrtc::rtp::packet::Packet) -> Result<Vec<u8>, anyhow::Error> {
    let expected_size = webrtc_util::MarshalSize::marshal_size(p);
    let mut data = vec![0; expected_size];
    let actual_size = webrtc_util::Marshal::marshal_to(p, &mut data)?;
    if expected_size != actual_size {
        anyhow::bail!("Wrong marshal size: expected {expected_size} but got {actual_size}");
    }
    Ok(data)
}


#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_streaming() -> anyhow::Result<()> {
        let video_file = "/home/linux/output.mp4";
        let file = std::fs::File::open(&video_file)
            .map_err(|e| anyhow::anyhow!("tried to open file {video_file} but got: {e:?}"))?;
        let size = file.metadata()?.len();
        let reader = std::io::BufReader::new(file);
        let mut mp4 = mp4::Mp4Reader::read_header(reader, size)?;
        let track_id = 1;
        // let mut adapter = crate::media::Mp4ReaderAdapter::new(mp4, track_id)?;
        // let mut h264 = H264Reader::new(adapter);
        let output = std::fs::File::create("/home/linux/teste.h264")?;
        let mut writer = std::io::BufWriter::new(output);
        // while let Ok(a) = h264.next_nal() {
        //     writer.write_all(&a.data)?;
        //     break;
        // }
        let mut bytes = [0u8; 8192];
        let sample_count = mp4.sample_count(track_id)?;
        for sample_id in 1..=sample_count {
            // let read = adapter.read(&mut bytes)?;
            let data = mp4.read_sample(track_id, sample_id)?.unwrap();
            // if read == 0 {
            //     break;
            // }
            // writer.write_all(&bytes[0..read])?;
            writer.write_all(&data.bytes)?;
        }
        Ok(())
    }
}
