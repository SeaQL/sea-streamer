use thiserror::Error;

use sea_streamer_kafka::KafkaErr;
use sea_streamer_stdio::StdioErr;
use sea_streamer_types::{StreamErr, StreamResult};

use crate::{Backend, SeaStreamerBackend};

pub type Error = StreamErr<BackendErr>;

#[derive(Error, Debug)]
pub enum BackendErr {
    #[error("KafkaBackendErr: {0}")]
    Kafka(KafkaErr),
    #[error("StdioBackendErr: {0}")]
    Stdio(StdioErr),
}

impl From<KafkaErr> for BackendErr {
    fn from(err: KafkaErr) -> Self {
        Self::Kafka(err)
    }
}

impl From<StdioErr> for BackendErr {
    fn from(err: StdioErr) -> Self {
        Self::Stdio(err)
    }
}

impl SeaStreamerBackend for BackendErr {
    fn backend(&self) -> Backend {
        match self {
            Self::Kafka(_) => Backend::Kafka,
            Self::Stdio(_) => Backend::Stdio,
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
        StreamErr::CommitNotAllowed => StreamErr::CommitNotAllowed,
        StreamErr::Utf8Error(e) => StreamErr::Utf8Error(e),
        StreamErr::InvalidStreamKey => StreamErr::InvalidStreamKey,
        StreamErr::Unsupported(e) => StreamErr::Unsupported(e),
        StreamErr::Runtime(e) => StreamErr::Runtime(e),
    }
}
