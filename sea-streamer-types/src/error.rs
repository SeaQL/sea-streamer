use std::str::Utf8Error;
use thiserror::Error;

/// Type alias of the [`Result`] type specific to `sea-streamer`.
pub type StreamResult<T, E> = std::result::Result<T, StreamErr<E>>;

#[derive(Error, Debug)]
/// Common errors that may occur.
pub enum StreamErr<E: std::error::Error> {
    #[error("Connection Error: {0}")]
    Connect(String),
    #[error("Timeout has not yet been set")]
    TimeoutNotSet,
    #[error("Producer has already been anchored")]
    AlreadyAnchored,
    #[error("Producer has not yet been anchored")]
    NotAnchored,
    #[error("Consumer group is set; but not expected")]
    ConsumerGroupIsSet,
    #[error("Consumer group has not yet been set")]
    ConsumerGroupNotSet,
    #[error("Stream key set is empty")]
    StreamKeyEmpty,
    #[error("You cannot commit on a real-time consumer")]
    CommitNotAllowed,
    #[error("Utf8Error: {0}")]
    Utf8Error(Utf8Error),
    #[error("Invalid stream key: valid pattern is [a-zA-Z0-9._-]{{1, 249}}")]
    InvalidStreamKey,
    #[error("Unsupported feature: {0}")]
    Unsupported(String),
    #[error("Backend error: {0}")]
    Backend(E),
    #[error("Runtime error: {0}")]
    Runtime(Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
#[derive(Error, Debug)]
/// Errors that may happen when processing JSON
pub enum JsonErr {
    #[error("Utf8Error {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("serde_json::Error {0}")]
    SerdeJson(#[from] serde_json::Error),
}

/// Function to construct a [`StreamErr::Runtime`] error variant.
pub fn runtime_error<T: std::error::Error, E: std::error::Error + Send + Sync + 'static>(
    e: E,
) -> StreamErr<T> {
    StreamErr::Runtime(Box::new(e))
}
