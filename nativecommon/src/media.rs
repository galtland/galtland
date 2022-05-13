use std::io::{Read, Seek};

use bytes::{Buf, Bytes, BytesMut};
use mp4::Mp4Sample;
use webrtc::rtp::packetizer::Payloader;


pub struct Mp4ReaderAdapter<T> {
    reader: mp4::Mp4Reader<T>,
    track_id: u32,
    sample_count: u32,
    current_sample: u32,
    pending_sample: Option<mp4::Mp4Sample>,
}


impl<T: Read + Seek> Mp4ReaderAdapter<T> {
    pub fn new(reader: mp4::Mp4Reader<T>, track_id: u32) -> anyhow::Result<Self> {
        let sample_count = reader.sample_count(track_id)?;
        Ok(Self {
            current_sample: 1,
            reader,
            sample_count,
            track_id,
            pending_sample: None,
        })
    }

    fn handle_sample(&mut self, mut sample: Mp4Sample, buf: &mut [u8]) -> usize {
        let sample_len = sample.bytes.len();
        let buf_len = buf.len();
        let current_sample = self.current_sample;
        let on_beginning = current_sample == 1 && sample.bytes.len() == sample.bytes.len();
        let len = if sample_len > buf_len {
            sample.bytes.copy_to_slice(buf);
            self.pending_sample.replace(sample);
            buf_len
        } else {
            self.current_sample += 1;
            buf[0..sample_len].copy_from_slice(&sample.bytes);
            sample_len
        };
        // for i in (0..len).step_by(2) {
        //     let n = buf[i];
        //     buf[i] = buf[i + 1];
        //     buf[i + 1] = n;
        // }
        if on_beginning {
            buf[2] = 0;
            buf[3] = 1; // fix h264 header
        }
        println!(
            "sample {}/{}",
            current_sample,
            self.sample_count,
            // hex::encode(&buf[0..len])
        );
        len
    }
}

impl<T: Read + Seek> Read for Mp4ReaderAdapter<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(sample) = self.pending_sample.take() {
            Ok(self.handle_sample(sample, buf))
        } else if self.current_sample > self.sample_count {
            Ok(0)
        } else {
            match self.reader.read_sample(self.track_id, self.current_sample) {
                Ok(Some(sample)) => Ok(self.handle_sample(sample, buf)),
                Ok(None) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Got None on read sample",
                )),
                Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Got error: {e:?}"),
                )),
            }
        }
    }
}

struct BytesMutReader {
    b: BytesMut,
}

impl Read for BytesMutReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.b.has_remaining() {
            return Ok(0);
        }
        let self_size = self.b.len();
        if self_size >= buf.len() {
            self.b.copy_to_slice(buf);
            Ok(buf.len())
        } else {
            buf[0..self_size].copy_from_slice(&self.b);
            self.b.clear();
            Ok(self_size)
        }
    }
}


const RTP_OUTBOUND_MTU: usize = 1200;

pub struct RtmpH264ToPackets {
    len_size: u8,
    h264_rate: u32,
    payload_type: u8,
    ssrc: u32,
    payloader: webrtc::rtp::codecs::h264::H264Payloader,
    sequencer: Box<dyn webrtc::rtp::sequence::Sequencer + Send + Sync>,
}

impl RtmpH264ToPackets {
    pub fn new(h264_rate: u32, payload_type: u8) -> Self {
        Self {
            h264_rate,
            payload_type,
            sequencer: Box::new(webrtc::rtp::sequence::new_random_sequencer()),
            len_size: 0,
            ssrc: 0, // TODO: change this?
            payloader: Default::default(),
        }
    }

    // Inspired by http://blog.kundansingh.com/2012/01/translating-h264-between-flash-player.html
    // TODO: consider rewriting using https://crates.io/crates/nom
    pub fn process(
        &mut self,
        data: &[u8],
        timestamp: u32,
    ) -> anyhow::Result<Vec<webrtc::rtp::packet::Packet>> {
        let mut data: BytesMut = data.into();
        if data.len() < 2 {
            anyhow::bail!("Data is too short: {} bytes", data.len());
        }
        let b0 = data.get_u8();
        let b1 = data.get_u8();
        if data.len() < 3 {
            anyhow::bail!("Video header is too short: {} bytes", data.len());
        }
        data.advance(3); // skip "delay/CompositionTime" field
        if b1 == 0x01 && (b0 == 0x17 || b0 == 0x27) {
            // AVC intra or inter
            self.process_nalus(data, timestamp)
        } else if b0 == 0x17 && b1 == 0x00 {
            // AVC seq configuration
            self.process_configuration(data, timestamp)
        } else {
            anyhow::bail!("Unrecognized first bytes: {b0:#04x} {b1:#04x}");
        }
    }

    fn process_nalus(
        &mut self,
        mut data: BytesMut,
        timestamp: u32,
    ) -> anyhow::Result<Vec<webrtc::rtp::packet::Packet>> {
        log::debug!("nalus data: {} bytes", data.len());
        let mut packets = Vec::new();
        while !data.is_empty() {
            if data.len() < self.len_size as usize {
                anyhow::bail!(
                    "Remaining data is too short for nal size: {} bytes",
                    data.len(),
                );
            }
            let nal_size = match self.len_size {
                1 => data.get_u8() as usize,
                2 => data.get_u16() as usize,
                4 => data.get_u32() as usize,
                other => anyhow::bail!("Invalid nal length size {other}"),
            };
            if data.len() < nal_size {
                anyhow::bail!(
                    "Remaining data is too short for nal: {} bytes < {} bytes",
                    data.len(),
                    nal_size
                );
            }
            let nal_data = data.copy_to_bytes(nal_size);
            packets.extend(self.packetize(&nal_data, timestamp)?);
        }
        Ok(packets)
    }

    fn process_configuration(
        &mut self,
        mut data: BytesMut,
        timestamp: u32,
    ) -> anyhow::Result<Vec<webrtc::rtp::packet::Packet>> {
        log::debug!("configuration data: {} bytes", data.len());
        if data.len() < 6 {
            anyhow::bail!("Data configuration is too short: {} bytes", data.len());
        }
        let version = data.get_u8();
        let profile_idc = data.get_u8();
        let profile_compat = data.get_u8();
        let level_idc = data.get_u8();
        if version != 1 {
            anyhow::bail!("Got config version {} which is unexpected", version);
        }
        self.len_size = (data.get_u8() & 0x03) + 1;
        log::info!("Configuration: version={version} profile_idc={profile_idc} profile_compat={profile_compat} level_idc={level_idc} len_size={}", self.len_size);

        let mut packets = Vec::new();
        let sps_count = data.get_u8() & 0x1f;
        log::debug!("Reading {sps_count} sps");
        for _ in 0..sps_count {
            if data.len() < 2 {
                anyhow::bail!("No bytes to read len sps: {} bytes", data.len());
            }
            let len_sps = data.get_u16() as usize;
            if data.len() < len_sps {
                anyhow::bail!(
                    "No bytes to read sps: {} bytes < {} bytes",
                    data.len(),
                    len_sps
                );
            }
            let sps_payload = data.copy_to_bytes(len_sps);
            packets.extend(self.packetize(&sps_payload, timestamp)?);
        }
        if data.is_empty() {
            anyhow::bail!("No bytes to read pps count: {} bytes", data.len());
        }
        let pps_count = data.get_u8();
        log::debug!("Reading {pps_count} pps");
        for _ in 0..pps_count {
            if data.len() < 2 {
                anyhow::bail!("No bytes to read len pps: {} bytes", data.len());
            }
            let len_pps = data.get_u16() as usize;
            if data.len() < len_pps {
                anyhow::bail!(
                    "No bytes to read pps: {} bytes < {} bytes",
                    data.len(),
                    len_pps
                );
            }
            let pps_payload = data.copy_to_bytes(len_pps);
            packets.extend(self.packetize(&pps_payload, timestamp)?);
        }
        Ok(packets)
    }

    // based on webrtc::rtp::packetizer
    fn packetize(
        &mut self,
        payload: &Bytes,
        rtmp_timestamp: u32,
    ) -> anyhow::Result<Vec<webrtc::rtp::packet::Packet>> {
        let timestamp = rtmp_timestamp * self.h264_rate / 1000;
        let payloads = self.payloader.payload(RTP_OUTBOUND_MTU - 12, payload)?;
        let payloads_len = payloads.len();
        let mut packets = Vec::with_capacity(payloads_len);
        for (i, payload) in payloads.into_iter().enumerate() {
            packets.push(webrtc::rtp::packet::Packet {
                header: webrtc::rtp::header::Header {
                    version: 2,
                    padding: false,
                    extension: false,
                    marker: i == payloads_len - 1,
                    payload_type: self.payload_type,
                    sequence_number: self.sequencer.next_sequence_number(),
                    timestamp,
                    ssrc: self.ssrc,
                    ..Default::default()
                },
                payload,
            });
        }
        Ok(packets)
    }
}

struct AudioTrackInfo {
    track_id: u8,
    frame_length: u16,
}

pub struct RtmpH264ToMp4<W> {
    len_size: u8,
    h264_rate: u32,
    h264_timescale: u32,
    audio_bitrate: u32,
    fps: f32,
    next_track: u8,
    video_track_id: Option<u8>,
    audio_track_info: Option<AudioTrackInfo>,
    writer: mp4::Mp4Writer<W>,
}

impl<W: std::io::Write + Seek> RtmpH264ToMp4<W> {
    pub fn new(h264_rate: u32, fps: f32, audio_bitrate: u32, writer: mp4::Mp4Writer<W>) -> Self {
        Self {
            h264_rate,
            writer,
            fps,
            audio_bitrate,
            h264_timescale: 15360,
            next_track: 1,
            len_size: 0,
            video_track_id: None,
            audio_track_info: None,
        }
    }

    // TODO: consider rewriting using https://crates.io/crates/nom
    pub fn process_video(&mut self, mut data: Bytes, timestamp: u32) -> anyhow::Result<()> {
        if data.len() < 2 {
            anyhow::bail!("Video data is too short: {} bytes", data.len());
        }
        let b0 = data.get_u8();
        let b1 = data.get_u8();
        if data.len() < 3 {
            anyhow::bail!("Video data header is too short: {} bytes", data.len());
        }
        let mut composition_time = (data.get_u8() as u32) << 16;
        composition_time |= (data.get_u8() as u32) << 8;
        composition_time |= data.get_u8() as u32;


        if b1 == 0x01 && (b0 == 0x17 || b0 == 0x27) {
            // AVC intra or inter
            self.process_h264_nalus(data, composition_time, timestamp)
        } else if b0 == 0x17 && b1 == 0x00 {
            // AVC seq configuration
            self.process_video_configuration(data)
        } else {
            anyhow::bail!("Unrecognized first bytes: {b0:#04x} {b1:#04x}");
        }
    }

    fn process_h264_nalus(
        &mut self,
        bytes: Bytes,
        composition_time: u32,
        timestamp: u32,
    ) -> anyhow::Result<()> {
        // let _ts_multipler = self.h264_rate / self.h264_timescale;
        log::debug!(
            "nalus data: {} bytes, timestamp {timestamp}, composition time {composition_time}",
            bytes.len()
        );
        if let Some(track_id) = self.video_track_id {
            let sample = mp4::Mp4Sample {
                start_time: (timestamp) as u64,
                duration: (self.h264_timescale as f32 / self.fps).round() as u32,
                rendering_offset: (composition_time) as i32,
                is_sync: false,
                bytes,
            };
            self.writer.write_sample(track_id as u32, &sample)?;
        } else {
            log::warn!("Ignoring video sample because no track id found");
        }
        Ok(())
    }

    fn process_video_configuration(&mut self, mut data: Bytes) -> anyhow::Result<()> {
        log::debug!("configuration data: {} bytes", data.len());
        if data.len() < 6 {
            anyhow::bail!("Data configuration is too short: {} bytes", data.len());
        }
        let version = data.get_u8();
        let profile_idc = data.get_u8();
        let profile_compat = data.get_u8();
        let level_idc = data.get_u8();
        if version != 1 {
            anyhow::bail!("Got config version {} which is unexpected", version);
        }
        self.len_size = (data.get_u8() & 0x03) + 1;
        log::info!("Video configuration: version={version} profile_idc={profile_idc} profile_compat={profile_compat} level_idc={level_idc} len_size={}", self.len_size);

        let mut sps = Vec::new();
        let sps_count = data.get_u8() & 0x1f;
        log::debug!("Reading {sps_count} sps");
        for _ in 0..sps_count {
            if data.len() < 2 {
                anyhow::bail!("No bytes to read len sps: {} bytes", data.len());
            }
            let len_sps = data.get_u16() as usize;
            if data.len() < len_sps {
                anyhow::bail!(
                    "No bytes to read sps: {} bytes < {} bytes",
                    data.len(),
                    len_sps
                );
            }
            let sps_payload = data.copy_to_bytes(len_sps);
            sps.extend(sps_payload);
        }

        if data.is_empty() {
            anyhow::bail!("No bytes to read pps count: {} bytes", data.len());
        }
        let mut pps = Vec::new();
        let pps_count = data.get_u8();
        log::debug!("Reading {pps_count} pps");
        for _ in 0..pps_count {
            if data.len() < 2 {
                anyhow::bail!("No bytes to read len pps: {} bytes", data.len());
            }
            let len_pps = data.get_u16() as usize;
            if data.len() < len_pps {
                anyhow::bail!(
                    "No bytes to read pps: {} bytes < {} bytes",
                    data.len(),
                    len_pps
                );
            }
            let pps_payload = data.copy_to_bytes(len_pps);
            pps.extend(pps_payload);
        }

        let video_track = mp4::TrackConfig {
            language: "und".to_owned(),
            timescale: self.h264_timescale,
            track_type: mp4::TrackType::Video,
            media_conf: mp4::MediaConfig::AvcConfig(mp4::AvcConfig {
                width: 1920,
                height: 1080,
                seq_param_set: sps,
                pic_param_set: pps,
            }),
        };
        self.writer.add_track(&video_track)?;

        self.video_track_id = Some(self.next_track);
        self.next_track += 1;

        Ok(())
    }

    pub fn process_audio(&mut self, mut data: Bytes, timestamp: u32) -> anyhow::Result<()> {
        if data.len() < 2 {
            anyhow::bail!("Audio data is too short: {} bytes", data.len());
        }
        let b0 = data.get_u8();
        if b0 & 0xf0 != 0xa0 {
            anyhow::bail!("Only AAC audio is supported, but received: {b0}");
        }
        // AAC
        match data.get_u8() {
            0 => {
                // AAC sequence header
                self.process_aac_configuration(data)?;
            }
            1 => {
                // AAC raw
                self.process_aac_raw_data(data, timestamp)?;
            }
            other => {
                anyhow::bail!("AAC Packet Type is unrecognized: {other}");
            }
        }
        Ok(())
    }

    fn process_aac_configuration(&mut self, mut data: Bytes) -> anyhow::Result<()> {
        let word = data.get_u16();
        let audio_object_type = (word >> 11) as u8; // get first 5 bits
        let audio_object_type = mp4::AudioObjectType::try_from(audio_object_type)?;

        let sampling_frequency_index = (word >> 7) as u8 & 0x0f; // get next 4 bits
        let sampling_frequency_index = mp4::SampleFreqIndex::try_from(sampling_frequency_index)?;

        let channel_configuration = (word >> 3) as u8 & 0x0f; // get next 4 bits
        let channel_configuration = mp4::ChannelConfig::try_from(channel_configuration)?;

        let frame_length: u16 = match data.get_u8() >> 7 {
            // get next bit
            0 => 1024,
            1 => 960,
            other => panic!("Found a bit with more than one value: {other}"),
        };

        let config = mp4::AacConfig {
            bitrate: self.audio_bitrate,
            profile: audio_object_type,
            freq_index: sampling_frequency_index,
            chan_conf: channel_configuration,
        };

        log::info!("Initializing audio track: {config:?} frame_length {frame_length}");

        let timescale = sampling_frequency_index.freq();
        let audio_track = mp4::TrackConfig {
            language: "und".to_owned(),
            timescale,
            track_type: mp4::TrackType::Audio,
            media_conf: mp4::MediaConfig::AacConfig(config),
        };
        self.writer.add_track(&audio_track)?;
        self.audio_track_info = Some(AudioTrackInfo {
            frame_length,
            track_id: self.next_track,
        });
        self.next_track += 1;

        Ok(())
    }

    fn process_aac_raw_data(&mut self, bytes: Bytes, timestamp: u32) -> anyhow::Result<()> {
        log::debug!("aac raw data: {} bytes, timestamp {timestamp}", bytes.len());
        if let Some(info) = &self.audio_track_info {
            let sample = mp4::Mp4Sample {
                start_time: (timestamp) as u64,
                duration: info.frame_length as u32,
                rendering_offset: 0,
                is_sync: false,
                bytes,
            };
            self.writer.write_sample(info.track_id as u32, &sample)?;
        } else {
            log::warn!("Ignoring audio sample because no track id found")
        }
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<W> {
        self.writer.write_end()?;
        Ok(self.writer.into_writer())
    }
}
