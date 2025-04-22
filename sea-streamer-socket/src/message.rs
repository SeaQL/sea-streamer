#[cfg(feature = "backend-file")]
use sea_streamer_file::FileMessage;
#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::KafkaMessage;
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::RedisMessage;
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioMessage;

use crate::{Backend, SeaStreamerBackend};
use sea_streamer_types::{Message, Payload, SeqNo, ShardId, StreamKey, Timestamp};

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of Message.
pub enum SeaMessage<'a> {
    #[cfg(feature = "backend-kafka")]
    Kafka(KafkaMessage<'a>),
    #[cfg(feature = "backend-redis")]
    Redis(RedisMessage),
    #[cfg(feature = "backend-stdio")]
    Stdio(StdioMessage),
    #[cfg(feature = "backend-file")]
    File(FileMessage),
    #[cfg(not(feature = "backend-kafka"))]
    None(std::marker::PhantomData<&'a ()>),
}

impl<'a> SeaStreamerBackend for SeaMessage<'a> {
    #[cfg(feature = "backend-kafka")]
    type Kafka = KafkaMessage<'a>;
    #[cfg(feature = "backend-redis")]
    type Redis = RedisMessage;
    #[cfg(feature = "backend-stdio")]
    type Stdio = StdioMessage;
    #[cfg(feature = "backend-file")]
    type File = FileMessage;

    fn backend(&self) -> Backend {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => Backend::Kafka,
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => Backend::Redis,
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => Backend::Stdio,
            #[cfg(feature = "backend-file")]
            Self::File(_) => Backend::File,
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => unreachable!(),
        }
    }

    #[cfg(feature = "backend-kafka")]
    fn get_kafka(&mut self) -> Option<&mut KafkaMessage<'a>> {
        match self {
            Self::Kafka(s) => Some(s),
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => None,
            #[cfg(feature = "backend-file")]
            Self::File(_) => None,
        }
    }

    #[cfg(feature = "backend-redis")]
    fn get_redis(&mut self) -> Option<&mut RedisMessage> {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => None,
            Self::Redis(s) => Some(s),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => None,
            #[cfg(feature = "backend-file")]
            Self::File(_) => None,
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => None,
        }
    }

    #[cfg(feature = "backend-stdio")]
    fn get_stdio(&mut self) -> Option<&mut StdioMessage> {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => None,
            Self::Stdio(s) => Some(s),
            #[cfg(feature = "backend-file")]
            Self::File(_) => None,
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => None,
        }
    }

    #[cfg(feature = "backend-file")]
    fn get_file(&mut self) -> Option<&mut FileMessage> {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => None,
            Self::File(s) => Some(s),
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => None,
        }
    }
}

impl Message for SeaMessage<'_> {
    fn stream_key(&self) -> StreamKey {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(i) => i.stream_key(),
            #[cfg(feature = "backend-redis")]
            Self::Redis(i) => i.stream_key(),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(i) => i.stream_key(),
            #[cfg(feature = "backend-file")]
            Self::File(i) => i.stream_key(),
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => unreachable!(),
        }
    }

    fn shard_id(&self) -> ShardId {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(i) => i.shard_id(),
            #[cfg(feature = "backend-redis")]
            Self::Redis(i) => i.shard_id(),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(i) => i.shard_id(),
            #[cfg(feature = "backend-file")]
            Self::File(i) => i.shard_id(),
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => unreachable!(),
        }
    }

    fn sequence(&self) -> SeqNo {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(i) => i.sequence(),
            #[cfg(feature = "backend-redis")]
            Self::Redis(i) => i.sequence(),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(i) => i.sequence(),
            #[cfg(feature = "backend-file")]
            Self::File(i) => i.sequence(),
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => unreachable!(),
        }
    }

    fn timestamp(&self) -> Timestamp {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(i) => i.timestamp(),
            #[cfg(feature = "backend-redis")]
            Self::Redis(i) => i.timestamp(),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(i) => i.timestamp(),
            #[cfg(feature = "backend-file")]
            Self::File(i) => i.timestamp(),
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => unreachable!(),
        }
    }

    fn message(&self) -> Payload {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(i) => i.message(),
            #[cfg(feature = "backend-redis")]
            Self::Redis(i) => i.message(),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(i) => i.message(),
            #[cfg(feature = "backend-file")]
            Self::File(i) => i.message(),
            #[cfg(not(feature = "backend-kafka"))]
            Self::None(_) => unreachable!(),
        }
    }
}
