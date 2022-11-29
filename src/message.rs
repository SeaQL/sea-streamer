use std::{str::Utf8Error, sync::Arc};

use crate::{SequenceNo, ShardId, StreamKey, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageFrame {
    meta: MessageMeta,
    bytes: Arc<Vec<u8>>,
    offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Message<'a> {
    meta: MessageMeta,
    mess: &'a [u8],
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
}

impl MessageFrame {
    pub fn new(meta: MessageMeta, bytes: Vec<u8>, offset: usize) -> Self {
        assert!(offset <= bytes.len());
        Self {
            meta,
            bytes: Arc::new(bytes),
            offset,
        }
    }

    pub fn meta(&self) -> &MessageMeta {
        &self.meta
    }

    pub fn message(&self) -> Message {
        Message {
            meta: self.meta.clone(),
            mess: &self.bytes[self.offset..],
        }
    }
}

impl<'a> Message<'a> {
    pub fn as_str(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(self.mess)
    }

    pub fn meta(&self) -> &MessageMeta {
        &self.meta
    }

    pub fn stream_key(&self) -> &StreamKey {
        self.meta.stream_key()
    }

    pub fn shard_id(&self) -> ShardId {
        *self.meta.shard_id()
    }

    pub fn sequence(&self) -> SequenceNo {
        *self.meta.sequence()
    }

    pub fn timestamp(&self) -> Timestamp {
        *self.meta.timestamp()
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

impl<'a> Sendable for Message<'a> {
    fn as_bytes(&self) -> &[u8] {
        self.mess
    }
}

impl Sendable for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Sendable for str {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}
