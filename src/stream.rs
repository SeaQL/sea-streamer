use std::{fmt::Display, str::FromStr};

pub use time::OffsetDateTime as Timestamp;

use crate::StreamErr;

pub const MAX_STREAM_KEY_LEN: usize = 249;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamKey {
    name: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId {
    id: u64,
}

pub type SequenceNo = u64;

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

impl FromStr for StreamKey {
    type Err = StreamErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() <= MAX_STREAM_KEY_LEN && s.chars().all(is_valid_stream_key_char) {
            Ok(StreamKey::new(s.to_owned()))
        } else {
            Err(StreamErr::InvalidStreamKey)
        }
    }
}

pub fn is_valid_stream_key_char(c: char) -> bool {
    // https://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}
