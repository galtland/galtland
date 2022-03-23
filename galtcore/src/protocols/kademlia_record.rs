// SPDX-License-Identifier: AGPL-3.0-only

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use libp2p::core::PublicKey;
use libp2p::kad::record::Key;
use libp2p::kad::Record;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use super::media_streaming::StreamingKey;

const SIMPLE_FILE_HASH_SIZE: usize = 32;
const STREAM_FILE_HASH_SIZE: usize = 32;

const STREAM_PREFIX: &[u8] = b"STREAM_";
const STREAM_KEY_SIZE: usize = STREAM_PREFIX.len() + STREAM_FILE_HASH_SIZE;

#[derive(Serialize, Deserialize)]
pub enum SerializableKademliaRecord {
    SimpleFileRecord {
        key: Vec<u8>,
        hash: Vec<u8>,
    },
    StreamingRecord {
        key: Vec<u8>,
        public_key: Vec<u8>,
        video_key: Vec<u8>,
        channel_key: Vec<u8>,
        updated_at_timestamp_seconds: i64,
        signature: Vec<u8>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum KademliaRecord {
    SimpleFileRecord { key: Key, hash: Bytes },
    MediaStreaming(StreamingRecord),
}

#[derive(Debug, Clone)]
pub struct StreamingRecord {
    pub key: Key,
    pub public_key: PublicKey,
    pub streaming_key: StreamingKey,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub signature: Bytes,
}

impl StreamingRecord {
    pub fn new(
        keypair: &libp2p::core::identity::Keypair,
        streaming_key: &StreamingKey,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Self> {
        if streaming_key.video_key.is_empty() {
            anyhow::bail!("Empty video key");
        }
        let kad_key = Self::streaming_kad_key(&streaming_key.video_key, &streaming_key.channel_key);
        let signature_data = Self::generate_record_signature_data(&kad_key, &updated_at);
        let signature = keypair.sign(&signature_data)?;
        let record = Self {
            key: kad_key.into(),
            public_key: keypair.public(),
            streaming_key: streaming_key.clone(),
            signature: signature.into(),
            updated_at,
        };
        Ok(record)
    }

    pub(crate) fn streaming_kad_key(app_name: &[u8], stream_key: &PeerId) -> Vec<u8> {
        let hash = StreamingKey::hash_from_parts(app_name, stream_key);
        assert!(hash.len() == STREAM_FILE_HASH_SIZE);
        [STREAM_PREFIX, &hash].concat()
    }

    pub(crate) fn generate_record_signature_data(
        kad_key: &[u8],
        updated_at: &DateTime<Utc>,
    ) -> Vec<u8> {
        [kad_key, updated_at.timestamp().to_be_bytes().as_slice()].concat()
    }
}

impl KademliaRecord {
    pub(crate) fn new_simple_file_record(bytes: &[u8]) -> Self {
        Self::SimpleFileRecord {
            key: Key::new(&bytes),
            hash: Bytes::copy_from_slice(bytes),
        }
    }

    pub(crate) fn into_record(self) -> anyhow::Result<Record> {
        match self {
            KademliaRecord::SimpleFileRecord { key, hash } => {
                let value = bincode::serialize(&SerializableKademliaRecord::SimpleFileRecord {
                    key: key.to_vec(),
                    hash: hash.to_vec(),
                })?;
                let record = Record {
                    key,
                    value,
                    publisher: None,
                    expires: None,
                };
                Ok(record)
            }
            KademliaRecord::MediaStreaming(StreamingRecord {
                key,
                public_key,
                streaming_key,
                updated_at,
                signature,
            }) => {
                let value = bincode::serialize(&SerializableKademliaRecord::StreamingRecord {
                    key: key.to_vec(),
                    public_key: public_key.to_protobuf_encoding(),
                    video_key: streaming_key.video_key,
                    channel_key: streaming_key.channel_key.to_bytes(),
                    updated_at_timestamp_seconds: updated_at.timestamp(),
                    signature: signature.to_vec(),
                })?;
                let record = Record {
                    key,
                    value,
                    publisher: None,
                    expires: None,
                };
                Ok(record)
            }
        }
    }

    pub(crate) fn key(&self) -> &Key {
        match self {
            Self::MediaStreaming(StreamingRecord { key, .. }) => key,
            Self::SimpleFileRecord { key, .. } => key,
        }
    }
}

impl TryFrom<&Record> for KademliaRecord {
    type Error = anyhow::Error;

    fn try_from(record: &Record) -> anyhow::Result<Self> {
        let v: SerializableKademliaRecord = bincode::deserialize(&record.value)?;

        match v {
            SerializableKademliaRecord::SimpleFileRecord { key, hash } => {
                if key.len() != hash.len() {
                    Err(anyhow::anyhow!("Key should be the same as hash"))
                } else if hash.len() != SIMPLE_FILE_HASH_SIZE {
                    Err(anyhow::anyhow!(
                        "Hash length is {} instead of {}",
                        hash.len(),
                        SIMPLE_FILE_HASH_SIZE
                    ))
                } else if key != record.key.to_vec() {
                    return Err(anyhow::anyhow!("Keys are inconsistent"));
                } else {
                    Ok(Self::SimpleFileRecord {
                        key: record.key.clone(),
                        hash: hash.into(),
                    })
                }
            }
            SerializableKademliaRecord::StreamingRecord {
                key,
                public_key,
                video_key: app_name,
                channel_key: stream_key,
                updated_at_timestamp_seconds,
                signature,
            } => {
                if key.len() != STREAM_KEY_SIZE {
                    anyhow::bail!("Key length is {} instead of {}", key.len(), STREAM_KEY_SIZE);
                }
                let public_key = libp2p::core::PublicKey::from_protobuf_encoding(&public_key)?;
                let peer_id = public_key.to_peer_id();

                let stream_key = match stream_key.try_into() {
                    Ok(p) => p,
                    Err(_) => anyhow::bail!("stream_key isn't a valid PeerId"),
                };

                if peer_id != stream_key {
                    anyhow::bail!("peer id {} should match stream key {}", peer_id, stream_key,);
                }
                let calculated_key = StreamingRecord::streaming_kad_key(&app_name, &stream_key);
                if calculated_key != key {
                    anyhow::bail!("Invalid key");
                }

                let updated_at = Utc.timestamp(updated_at_timestamp_seconds, 0);
                let signature_data =
                    StreamingRecord::generate_record_signature_data(&calculated_key, &updated_at);
                if !public_key.verify(&signature_data, &signature) {
                    anyhow::bail!("Invalid signature");
                }
                let streaming_key = StreamingKey {
                    video_key: app_name,
                    channel_key: stream_key,
                };
                Ok(Self::MediaStreaming(StreamingRecord {
                    key: record.key.clone(),
                    public_key,
                    streaming_key,
                    updated_at,
                    signature: signature.into(),
                }))
            }
        }
    }
}
