use thiserror::Error;

use sea_streamer_kafka::KafkaErr;
use sea_streamer_stdio::StdioErr;
use sea_streamer_types::{StreamErr, StreamResult};

use crate::{Backend, SeaStreamerBackend};

/// `sea-streamer-socket` the concrete error type.
pub type Error = StreamErr<BackendErr>;

#[derive(Error, Debug)]
/// `sea-streamer-socket` the concrete backend error.
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
    type Kafka = KafkaErr;
    type Stdio = StdioErr;

    fn backend(&self) -> Backend {
        match self {
            Self::Kafka(_) => Backend::Kafka,
            Self::Stdio(_) => Backend::Stdio,
        }
    }

    fn get_kafka(&mut self) -> Option<&mut KafkaErr> {
        match self {
            Self::Kafka(s) => Some(s),
            Self::Stdio(_) => None,
        }
    }

    fn get_stdio(&mut self) -> Option<&mut StdioErr> {
        match self {
            Self::Kafka(_) => None,
            Self::Stdio(s) => Some(s),
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
        StreamErr::ConsumerNotAssigned => StreamErr::ConsumerNotAssigned,
        StreamErr::StreamKeyEmpty => StreamErr::StreamKeyEmpty,
        StreamErr::CommitNotAllowed => StreamErr::CommitNotAllowed,
        StreamErr::Utf8Error(e) => StreamErr::Utf8Error(e),
        StreamErr::StreamKeyErr(e) => StreamErr::StreamKeyErr(e),
        StreamErr::Unsupported(e) => StreamErr::Unsupported(e),
        StreamErr::Runtime(e) => StreamErr::Runtime(e),
    }
}
