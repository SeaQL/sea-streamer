#[cfg(feature = "backend-file")]
use sea_streamer_file::FileErr;
#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::KafkaErr;
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::RedisErr;
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioErr;

use crate::{Backend, SeaStreamerBackend};
use sea_streamer_types::{StreamErr, StreamResult};
use thiserror::Error;

/// `sea-streamer-socket` the concrete error type.
pub type Error = StreamErr<BackendErr>;

#[derive(Error, Debug)]
/// `sea-streamer-socket` the concrete backend error.
pub enum BackendErr {
    #[cfg(feature = "backend-kafka")]
    #[error("KafkaBackendErr: {0}")]
    Kafka(KafkaErr),
    #[cfg(feature = "backend-redis")]
    #[error("RedisBackendErr: {0}")]
    Redis(RedisErr),
    #[cfg(feature = "backend-stdio")]
    #[error("StdioBackendErr: {0}")]
    Stdio(StdioErr),
    #[cfg(feature = "backend-file")]
    #[error("FileBackendErr: {0}")]
    File(FileErr),
}

#[cfg(feature = "backend-kafka")]
impl From<KafkaErr> for BackendErr {
    fn from(err: KafkaErr) -> Self {
        Self::Kafka(err)
    }
}

#[cfg(feature = "backend-redis")]
impl From<RedisErr> for BackendErr {
    fn from(err: RedisErr) -> Self {
        Self::Redis(err)
    }
}

#[cfg(feature = "backend-stdio")]
impl From<StdioErr> for BackendErr {
    fn from(err: StdioErr) -> Self {
        Self::Stdio(err)
    }
}

#[cfg(feature = "backend-file")]
impl From<FileErr> for BackendErr {
    fn from(err: FileErr) -> Self {
        Self::File(err)
    }
}

impl SeaStreamerBackend for BackendErr {
    #[cfg(feature = "backend-kafka")]
    type Kafka = KafkaErr;
    #[cfg(feature = "backend-redis")]
    type Redis = RedisErr;
    #[cfg(feature = "backend-stdio")]
    type Stdio = StdioErr;
    #[cfg(feature = "backend-file")]
    type File = FileErr;

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
        }
    }

    #[cfg(feature = "backend-kafka")]
    fn get_kafka(&mut self) -> Option<&mut KafkaErr> {
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
    fn get_redis(&mut self) -> Option<&mut RedisErr> {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => None,
            Self::Redis(s) => Some(s),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => None,
            #[cfg(feature = "backend-file")]
            Self::File(_) => None,
        }
    }

    #[cfg(feature = "backend-stdio")]
    fn get_stdio(&mut self) -> Option<&mut StdioErr> {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => None,
            Self::Stdio(s) => Some(s),
            #[cfg(feature = "backend-file")]
            Self::File(_) => None,
        }
    }

    #[cfg(feature = "backend-file")]
    fn get_file(&mut self) -> Option<&mut FileErr> {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => None,
            Self::File(s) => Some(s),
        }
    }
}

pub(crate) type SeaResult<T> = StreamResult<T, BackendErr>;

pub(crate) fn map_err<E: std::error::Error + Into<BackendErr>>(
    err: StreamErr<E>,
) -> StreamErr<BackendErr> {
    match err {
        StreamErr::Backend(err) => StreamErr::Backend(err.into()),
        // sadly here is a lot of boilerplate, but at least the compiler tells us when it breaks
        StreamErr::Connect(e) => StreamErr::Connect(e),
        StreamErr::TimeoutNotSet => StreamErr::TimeoutNotSet,
        StreamErr::AlreadyAnchored => StreamErr::AlreadyAnchored,
        StreamErr::NotAnchored => StreamErr::NotAnchored,
        StreamErr::ConsumerGroupIsSet => StreamErr::ConsumerGroupIsSet,
        StreamErr::ConsumerGroupNotSet => StreamErr::ConsumerGroupNotSet,
        StreamErr::StreamKeyEmpty => StreamErr::StreamKeyEmpty,
        StreamErr::StreamKeyNotFound => StreamErr::StreamKeyNotFound,
        StreamErr::CommitNotAllowed => StreamErr::CommitNotAllowed,
        StreamErr::Utf8Error(e) => StreamErr::Utf8Error(e),
        StreamErr::StreamUrlErr(e) => StreamErr::StreamUrlErr(e),
        StreamErr::StreamKeyErr(e) => StreamErr::StreamKeyErr(e),
        StreamErr::Unsupported(e) => StreamErr::Unsupported(e),
        StreamErr::Runtime(e) => StreamErr::Runtime(e),
    }
}
