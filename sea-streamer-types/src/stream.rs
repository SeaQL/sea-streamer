use std::{fmt::Display, str::FromStr};
use thiserror::Error;
pub use time::OffsetDateTime as Timestamp;

use crate::StreamErr;

/// Maximum string length of a stream key.
pub const MAX_STREAM_KEY_LEN: usize = 249;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Identifies a stream. Aka. topic.
pub struct StreamKey {
    name: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Identifies a shard. Aka. partition.
pub struct ShardId {
    id: u64,
}

/// The tuple (StreamKey, ShardId, SequenceNo) uniquely identifies a message. Aka. offset.
pub type SequenceNo = u64;

#[derive(Debug)]
/// Identifies a position in a stream.
pub enum SequencePos {
    Beginning,
    End,
    At(SequenceNo),
}

impl StreamKey {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ShardId {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Display for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Error, Debug)]
/// Placeholder type. Will never be constructed.
pub enum NeverErr {}

impl FromStr for StreamKey {
    type Err = StreamErr<NeverErr>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() <= MAX_STREAM_KEY_LEN && s.chars().all(is_valid_stream_key_char) {
            Ok(StreamKey::new(s.to_owned()))
        } else {
            Err(StreamErr::InvalidStreamKey)
        }
    }
}

/// Returns true if this character can be used in a stream key.
pub fn is_valid_stream_key_char(c: char) -> bool {
    // https://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}
