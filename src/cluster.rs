use crate::{
    error::Result, ConnectOptions, Consumer, ConsumerOptions, Producer, ProducerOptions, StreamKey,
};
use async_trait::async_trait;
use url::Url;

#[derive(Debug)]
pub struct ClusterUri {
    pub nodes: Vec<Url>,
}

#[async_trait]
pub trait Cluster: Sized {
    type Producer: Producer;
    type Consumer: Consumer;
    type ConnectOptions: ConnectOptions;
    type ConsumerOptions: ConsumerOptions;
    type ProducerOptions: ProducerOptions;

    async fn connect(cluster: ClusterUri, options: Self::ConnectOptions) -> Result<Self>;

    fn create_generic_producer(options: Self::ProducerOptions) -> Result<Self::Producer>;

    async fn create_producer(
        stream: StreamKey,
        options: Self::ProducerOptions,
    ) -> Result<Self::Producer> {
        let mut producer = Self::create_generic_producer(options)?;
        producer.anchor(stream)?;
        Ok(producer)
    }

    async fn create_consumer(
        stream: StreamKey,
        options: Self::ConsumerOptions,
    ) -> Result<Self::Consumer>;
}
