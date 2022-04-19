use std::io::{Read, Seek};

use bytes::Buf;
use mp4::Mp4Sample;


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
        let sample_len = sample.bytes.remaining();
        let buf_len = buf.len();
        let current_sample = self.current_sample;
        let on_beginning = current_sample == 1 && sample.bytes.len() == sample.bytes.remaining();
        let len = if sample_len > buf_len {
            sample.bytes.copy_to_slice(buf);
            self.pending_sample.replace(sample);
            buf_len
        } else {
            self.current_sample += 1;
            buf[0..sample_len].copy_from_slice(&mut sample.bytes);
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
