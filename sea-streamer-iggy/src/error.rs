use thiserror::Error;

#[derive(Error, Debug)]
pub enum IggyErr {
    #[error("Iggy error: {0}")]
    Client(#[from] iggy::prelude::IggyError),
    #[error("Stream and topic must be specified")]
    StreamTopicRequired,
    #[error("Channel receive error")]
    ChannelRecv,
    #[error("{0}")]
    Generic(String),
}
