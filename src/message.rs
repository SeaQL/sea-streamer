use std::{str::Utf8Error, sync::Arc};

use crate::{SequenceNo, ShardId, StreamKey, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SharedMessage {
    meta: MessageMeta,
    bytes: Arc<Vec<u8>>,
    offset: u32,
    length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Payload<'a> {
    bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageMeta {
    stream_key: StreamKey,
    shard_id: ShardId,
    sequence: SequenceNo,
    timestamp: Timestamp,
}

pub trait Sendable {
    fn as_bytes(&self) -> &[u8];

    fn as_str(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(self.as_bytes())
    }
}

pub trait Message {
    fn stream_key(&self) -> StreamKey;

    fn shard_id(&self) -> ShardId;

    fn sequence(&self) -> SequenceNo;

    fn timestamp(&self) -> Timestamp;

    fn message(&self) -> Payload;
}

impl SharedMessage {
    pub fn new(meta: MessageMeta, bytes: Vec<u8>, offset: usize, length: usize) -> Self {
        assert!(offset <= bytes.len());
        Self {
            meta,
            bytes: Arc::new(bytes),
            offset: offset as u32,
            length: length as u32,
        }
    }
}

impl Message for SharedMessage {
    fn stream_key(&self) -> StreamKey {
        self.meta.stream_key().to_owned()
    }

    fn shard_id(&self) -> ShardId {
        *self.meta.shard_id()
    }

    fn sequence(&self) -> SequenceNo {
        *self.meta.sequence()
    }

    fn timestamp(&self) -> Timestamp {
        *self.meta.timestamp()
    }

    fn message(&self) -> Payload {
        Payload {
            bytes: &self.bytes[self.offset as usize..(self.offset + self.length) as usize],
        }
    }
}

impl MessageMeta {
    pub fn new(
        stream_key: StreamKey,
        shard_id: ShardId,
        sequence: SequenceNo,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            stream_key,
            shard_id,
            sequence,
            timestamp,
        }
    }

    pub fn stream_key(&self) -> &StreamKey {
        &self.stream_key
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    pub fn sequence(&self) -> &SequenceNo {
        &self.sequence
    }

    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }
}

impl<'a> Sendable for &Payload<'a> {
    fn as_bytes(&self) -> &[u8] {
        self.bytes
    }
}

impl<T: AsRef<str>> Sendable for T {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
}

impl<'a> Payload<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    #[cfg(feature = "json")]
    pub fn deserialize_json<D: serde::de::DeserializeOwned>(&self) -> Result<D, crate::JsonErr> {
        Ok(serde_json::from_str(self.as_str()?)?)
    }
}
