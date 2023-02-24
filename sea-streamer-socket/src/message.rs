use sea_streamer_kafka::KafkaMessage;
use sea_streamer_stdio::StdioMessage;
use sea_streamer_types::{Message, Payload, SeqNo, ShardId, StreamKey, Timestamp};

use crate::{Backend, SeaStreamerBackend};

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of Message.
pub enum SeaMessage<'a> {
    Kafka(KafkaMessage<'a>),
    Stdio(StdioMessage),
}

impl<'a> SeaStreamerBackend for SeaMessage<'a> {
    fn backend(&self) -> Backend {
        match self {
            Self::Kafka(_) => Backend::Kafka,
            Self::Stdio(_) => Backend::Stdio,
        }
    }
}

impl<'a> Message for SeaMessage<'a> {
    fn stream_key(&self) -> StreamKey {
        match self {
            Self::Kafka(i) => i.stream_key(),
            Self::Stdio(i) => i.stream_key(),
        }
    }

    fn shard_id(&self) -> ShardId {
        match self {
            Self::Kafka(i) => i.shard_id(),
            Self::Stdio(i) => i.shard_id(),
        }
    }

    fn sequence(&self) -> SeqNo {
        match self {
            Self::Kafka(i) => i.sequence(),
            Self::Stdio(i) => i.sequence(),
        }
    }

    fn timestamp(&self) -> Timestamp {
        match self {
            Self::Kafka(i) => i.timestamp(),
            Self::Stdio(i) => i.timestamp(),
        }
    }

    fn message(&self) -> Payload {
        match self {
            Self::Kafka(i) => i.message(),
            Self::Stdio(i) => i.message(),
        }
    }
}
