use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, StreamErr>;

#[derive(Error, Debug)]
pub enum StreamErr {
    #[error("Cannot reach cluster")]
    ConnectionError,
    #[error("This producer has already been anchored")]
    AlreadyAnchored,
    #[error("This producer has not yet been anchored")]
    NotAnchored,
    #[error("Consumer group has not yet been set")]
    ConsumerGroupNotSet,
}
