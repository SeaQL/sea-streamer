use std::{fmt::Display, str::FromStr, sync::Arc};
pub use time::OffsetDateTime as Timestamp;

use crate::StreamKeyErr;

/// Maximum string length of a stream key.
pub const MAX_STREAM_KEY_LEN: usize = 249;

/// Reserved by SeaStreamer. Avoid using this as StreamKey.
pub const SEA_STREAMER_INTERNAL: &str = "SEA_STREAMER_INTERNAL";

/// Canonical display format for Timestamp.
pub const TIMESTAMP_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Identifies a stream. Aka. topic.
pub struct StreamKey {
    name: Arc<String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Identifies a shard. Aka. partition.
pub struct ShardId {
    id: u64,
}

/// The tuple (StreamKey, ShardId, SeqNo) uniquely identifies a message. Aka. offset.
pub type SeqNo = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Identifies a position in a stream.
pub enum SeqPos {
    Beginning,
    End,
    At(SeqNo),
}

impl StreamKey {
    pub fn new<S: Into<String>>(key: S) -> Result<Self, StreamKeyErr> {
        let key = key.into();
        if is_valid_stream_key(key.as_str()) {
            Ok(Self {
                name: Arc::new(key),
            })
        } else {
            Err(StreamKeyErr::InvalidStreamKey)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ShardId {
    pub const fn new(id: u64) -> Self {
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

impl FromStr for StreamKey {
    type Err = StreamKeyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        StreamKey::new(s)
    }
}

pub fn is_valid_stream_key(s: &str) -> bool {
    s.len() <= MAX_STREAM_KEY_LEN && s.chars().all(is_valid_stream_key_char)
}

/// Returns true if this character can be used in a stream key.
pub fn is_valid_stream_key_char(c: char) -> bool {
    // https://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}
