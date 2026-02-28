use redis::{ErrorKind, RedisError, ServerErrorKind};
use sea_streamer_types::{StreamErr, StreamResult};
use thiserror::Error;

#[derive(Error, Debug, Clone)]
/// Different types of Redis errors.
pub enum RedisErr {
    #[error("Failed to parse message ID: {0}")]
    MessageId(String),
    #[error("Failed to parse StreamReadReply: {0:?}")]
    StreamReadReply(String),
    #[error("The Producer task died")]
    ProducerDied,
    #[error("Consumer died with unrecoverable error. Check the log for details.")]
    ConsumerDied,
    #[error("The server generated an invalid response: {0}")]
    ResponseError(String),
    #[error("The authentication with the server failed: {0}")]
    AuthenticationFailed(String),
    #[error("Operation failed because of a type mismatch: {0}")]
    TypeError(String),
    #[error("A script execution was aborted: {0}")]
    ExecAbortError(String),
    #[error("The server cannot response because it's loading a dump: {0}")]
    BusyLoadingError(String),
    #[error("A script that was requested does not actually exist: {0}")]
    NoScriptError(String),
    #[error("An error that was caused because the parameter to the client were wrong: {0}")]
    InvalidClientConfig(String),
    #[error("Raised if a key moved to a different node: {0}")]
    Moved(String),
    #[error("Raised if a key moved to a different node but we need to ask: {0}")]
    Ask(String),
    #[error("Raised if a request needs to be retried: {0}")]
    TryAgain(String),
    #[error("Raised if a redis cluster is down: {0}")]
    ClusterDown(String),
    #[error("A request spans multiple slots: {0}")]
    CrossSlot(String),
    #[error("A cluster master is unavailable: {0}")]
    MasterDown(String),
    #[error("IO error: {0}")]
    IoError(String),
    #[error("An error raised that was identified on the client before execution: {0}")]
    ClientError(String),
    #[error("Extension error: {0}")]
    ExtensionError(String),
    #[error("Attempt to write to a read-only server: {0}")]
    ReadOnly(String),
    #[error("Unknown error: {0}")]
    Unknown(String),
}

/// A type alias for convenience.
pub type RedisResult<T> = StreamResult<T, RedisErr>;

pub(crate) fn map_err(err: RedisError) -> StreamErr<RedisErr> {
    let e = format!("{err}");
    StreamErr::Backend(match err.kind() {
        ErrorKind::AuthenticationFailed => RedisErr::AuthenticationFailed(e),
        ErrorKind::UnexpectedReturnType => RedisErr::TypeError(e),
        ErrorKind::InvalidClientConfig => RedisErr::InvalidClientConfig(e),
        ErrorKind::Io => RedisErr::IoError(e),
        ErrorKind::Client => RedisErr::ClientError(e),
        ErrorKind::Extension => RedisErr::ExtensionError(e),
        ErrorKind::Server(ServerErrorKind::ResponseError) => RedisErr::ResponseError(e),
        ErrorKind::Server(ServerErrorKind::ExecAbort) => RedisErr::ExecAbortError(e),
        ErrorKind::Server(ServerErrorKind::BusyLoading) => RedisErr::BusyLoadingError(e),
        ErrorKind::Server(ServerErrorKind::NoScript) => RedisErr::NoScriptError(e),
        ErrorKind::Server(ServerErrorKind::Moved) => RedisErr::Moved(e),
        ErrorKind::Server(ServerErrorKind::Ask) => RedisErr::Ask(e),
        ErrorKind::Server(ServerErrorKind::TryAgain) => RedisErr::TryAgain(e),
        ErrorKind::Server(ServerErrorKind::ClusterDown) => RedisErr::ClusterDown(e),
        ErrorKind::Server(ServerErrorKind::CrossSlot) => RedisErr::CrossSlot(e),
        ErrorKind::Server(ServerErrorKind::MasterDown) => RedisErr::MasterDown(e),
        ErrorKind::Server(ServerErrorKind::ReadOnly) => RedisErr::ReadOnly(e),
        _ => RedisErr::Unknown(e),
    })
}
