// SPDX-License-Identifier: AGPL-3.0-only

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use libp2p::core::PublicKey;
use libp2p::kad::record::Key;
use libp2p::kad::Record;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use super::rtmp_streaming::RtmpStreamingKey;

const SIMPLE_FILE_HASH_SIZE: usize = 32;
const RTMP_FILE_HASH_SIZE: usize = 32;

const RTMP_PREFIX: &[u8] = b"rtmp_";
const RTMP_KEY_SIZE: usize = RTMP_PREFIX.len() + RTMP_FILE_HASH_SIZE;

#[derive(Serialize, Deserialize)]
pub enum SerializableKademliaRecord {
    SimpleFileRecord {
        key: Vec<u8>,
        hash: Vec<u8>,
    },
    RtmpStreamingRecord {
        key: Vec<u8>,
        public_key: Vec<u8>,
        app_name: String,
        stream_key: Vec<u8>,
        updated_at_timestamp_seconds: i64,
        signature: Vec<u8>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum KademliaRecord {
    SimpleFileRecord { key: Key, hash: Bytes },
    Rtmp(RtmpStreamingRecord),
}

#[derive(Debug, Clone)]
pub struct RtmpStreamingRecord {
    pub key: Key,
    pub public_key: PublicKey,
    pub streaming_key: RtmpStreamingKey,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub signature: Bytes,
}

impl RtmpStreamingRecord {
    pub fn new(
        keypair: &libp2p::core::identity::Keypair,
        streaming_key: &RtmpStreamingKey,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Self> {
        if streaming_key.app_name.is_empty() {
            anyhow::bail!("Empty app name");
        }
        let kad_key =
            Self::rtmp_streaming_kad_key(&streaming_key.app_name, &streaming_key.stream_key);
        let signature_data = Self::generate_rtmp_record_signature_data(&kad_key, &updated_at);
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

    pub fn rtmp_streaming_kad_key(app_name: &str, stream_key: &PeerId) -> Vec<u8> {
        let hash = RtmpStreamingKey::hash_from_parts(app_name.as_bytes(), stream_key);
        assert!(hash.len() == RTMP_FILE_HASH_SIZE);
        [RTMP_PREFIX, &hash].concat()
    }

    pub fn generate_rtmp_record_signature_data(
        kad_key: &[u8],
        updated_at: &DateTime<Utc>,
    ) -> Vec<u8> {
        [kad_key, updated_at.timestamp().to_be_bytes().as_slice()].concat()
    }
}

impl KademliaRecord {
    pub fn new_simple_file_record(bytes: &[u8]) -> Self {
        Self::SimpleFileRecord {
            key: Key::new(&bytes),
            hash: Bytes::copy_from_slice(bytes),
        }
    }

    pub fn into_record(self) -> anyhow::Result<Record> {
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
            KademliaRecord::Rtmp(RtmpStreamingRecord {
                key,
                public_key,
                streaming_key,
                updated_at,
                signature,
            }) => {
                let value = bincode::serialize(&SerializableKademliaRecord::RtmpStreamingRecord {
                    key: key.to_vec(),
                    public_key: public_key.to_protobuf_encoding(),
                    app_name: streaming_key.app_name,
                    stream_key: streaming_key.stream_key.to_bytes(),
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

    pub fn key(&self) -> &Key {
        match self {
            Self::Rtmp(RtmpStreamingRecord { key, .. }) => key,
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
            SerializableKademliaRecord::RtmpStreamingRecord {
                key,
                public_key,
                app_name,
                stream_key,
                updated_at_timestamp_seconds,
                signature,
            } => {
                if key.len() != RTMP_KEY_SIZE {
                    anyhow::bail!("Key length is {} instead of {}", key.len(), RTMP_KEY_SIZE);
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
                let calculated_key =
                    RtmpStreamingRecord::rtmp_streaming_kad_key(&app_name, &stream_key);
                if calculated_key != key {
                    anyhow::bail!("Invalid key");
                }

                let updated_at = Utc.timestamp(updated_at_timestamp_seconds, 0);
                let signature_data = RtmpStreamingRecord::generate_rtmp_record_signature_data(
                    &calculated_key,
                    &updated_at,
                );
                if !public_key.verify(&signature_data, &signature) {
                    anyhow::bail!("Invalid signature");
                }
                let streaming_key = RtmpStreamingKey {
                    app_name,
                    stream_key,
                };
                Ok(Self::Rtmp(RtmpStreamingRecord {
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
