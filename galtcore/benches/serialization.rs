use criterion::{black_box, criterion_group, criterion_main, Criterion};
use galtcore::protocols::media_streaming::{
    IntegerStreamTrack, StreamMetadata, StreamOffset, StreamingData, StreamingDataType,
};
use rand::Rng;


fn create_fixture() -> StreamingData {
    let mut data = [0u8; 1024];
    rand::thread_rng().fill(&mut data);
    let stream_track = IntegerStreamTrack {
        stream_id: rand::thread_rng().gen(),
        track_id: rand::thread_rng().gen(),
    };
    let metadata = StreamMetadata {
        capability: Default::default(),
    };
    let source_offset = StreamOffset {
        timestamp_reference: rand::thread_rng().gen(),
        sequence_id: rand::thread_rng().gen(),
    };
    StreamingData {
        data: data.into(),
        stream_track,
        metadata: Some(metadata),
        source_offset,
        data_type: StreamingDataType::WebRtcRtpPacket,
    }
}

fn serialize_bincode(data: &StreamingData) -> Vec<u8> {
    bincode::serialize(data).expect("to work")
}

fn serialize_rmp_serde(data: &StreamingData) -> Vec<u8> {
    rmp_serde::to_vec(data).expect("to work")
}

fn serialize_cbor(data: &StreamingData) -> Vec<u8> {
    let mut writer = Vec::new();
    ciborium::ser::into_writer(&data, &mut writer).unwrap();
    writer
}

fn deserialize_bincode(data: &[u8]) -> StreamingData {
    bincode::deserialize(data).expect("to work")
}

fn deserialize_rmp_serde(data: &[u8]) -> StreamingData {
    rmp_serde::from_slice(data).expect("to work")
}

fn deserialize_cbor(data: &[u8]) -> StreamingData {
    ciborium::de::from_reader(data).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let data = create_fixture();
    let data_bincode = serialize_bincode(&data);
    let data_rmp_serde = serialize_rmp_serde(&data);
    let data_cbor = serialize_cbor(&data);

    c.bench_function("serialize_bincode", |b| {
        b.iter(|| serialize_bincode(black_box(&data)))
    });
    c.bench_function("serialize_rmp_serde", |b| {
        b.iter(|| serialize_rmp_serde(black_box(&data)))
    });
    c.bench_function("serialize_cbor", |b| {
        b.iter(|| serialize_cbor(black_box(&data)))
    });

    c.bench_function("deserialize_bincode", |b| {
        b.iter(|| deserialize_bincode(black_box(&data_bincode)))
    });
    c.bench_function("deserialize_rmp_serde", |b| {
        b.iter(|| deserialize_rmp_serde(black_box(&data_rmp_serde)))
    });
    c.bench_function("deserialize_cbor", |b| {
        b.iter(|| deserialize_cbor(black_box(&data_cbor)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
