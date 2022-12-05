use crate::{
    ConnectOptions, Consumer, ConsumerOptions, Producer, ProducerOptions, Result, StreamKey,
};
use async_trait::async_trait;
use url::Url;

#[derive(Debug)]
pub struct StreamerUri {
    pub nodes: Vec<Url>,
}

#[async_trait]
pub trait Streamer: Sized {
    type Producer: Producer;
    type Consumer: Consumer;
    type ConnectOptions: ConnectOptions;
    type ConsumerOptions: ConsumerOptions;
    type ProducerOptions: ProducerOptions;

    async fn connect(streamer: StreamerUri, options: Self::ConnectOptions) -> Result<Self>;

    fn create_generic_producer(&self, options: Self::ProducerOptions) -> Result<Self::Producer>;

    async fn create_producer(
        &self,
        stream: StreamKey,
        options: Self::ProducerOptions,
    ) -> Result<Self::Producer> {
        let mut producer = self.create_generic_producer(options)?;
        producer.anchor(stream)?;
        Ok(producer)
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> Result<Self::Consumer>;
}

impl StreamerUri {
    pub fn zero() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn one(url: Url) -> Self {
        Self { nodes: vec![url] }
    }
}
