use crate::Value;
use sea_streamer_types::StreamResult;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedisErr {
    #[error("Failed to parse message ID: {0}")]
    MessageId(String),
    #[error("Failed to parse StreamReadReply: {0:?}")]
    StreamReadReply(Value),
}

pub type RedisResult<T> = StreamResult<T, RedisErr>;
