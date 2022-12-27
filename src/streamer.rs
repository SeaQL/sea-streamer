use crate::{
    ConnectOptions, Consumer, ConsumerOptions, Producer, ProducerOptions, StreamKey, StreamResult,
};
use async_trait::async_trait;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamerUri {
    pub nodes: Vec<Url>,
}

#[async_trait]
pub trait Streamer: Sized {
    type Error: std::error::Error;
    type Producer: Producer<Error = Self::Error>;
    type Consumer: Consumer<Error = Self::Error>;
    type ConnectOptions: ConnectOptions;
    type ConsumerOptions: ConsumerOptions;
    type ProducerOptions: ProducerOptions;

    async fn connect(
        streamer: StreamerUri,
        options: Self::ConnectOptions,
    ) -> StreamResult<Self, Self::Error>;

    async fn disconnect(self) -> StreamResult<(), Self::Error>;

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer, Self::Error>;

    async fn create_producer(
        &self,
        stream: StreamKey,
        options: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer, Self::Error> {
        let mut producer = self.create_generic_producer(options).await?;
        producer.anchor(stream)?;
        Ok(producer)
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> StreamResult<Self::Consumer, Self::Error>;
}

impl StreamerUri {
    pub fn zero() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn one(url: Url) -> Self {
        Self { nodes: vec![url] }
    }

    pub fn many(urls: impl Iterator<Item = Url>) -> Self {
        Self {
            nodes: urls.collect(),
        }
    }
}
