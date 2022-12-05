use async_trait::async_trait;
use std::time::Duration;

use crate::{create_consumer, StdioConsumer, StdioProducer};
use sea_streamer::{
    ConnectOptions as ConnectOptionsTrait, ConsumerGroup, ConsumerMode,
    ConsumerOptions as ConsumerOptionsTrait, ProducerOptions as ProducerOptionsTrait, StreamErr,
    StreamKey, StreamResult, Streamer as StreamerTrait, StreamerUri,
};

#[derive(Debug, Default, Clone)]
pub struct StdioStreamer {}

#[derive(Debug, Default, Clone)]
pub struct StdioConnectOptions {}

#[derive(Debug, Default, Clone)]
pub struct StdioConsumerOptions {
    group: Option<ConsumerGroup>,
}

#[derive(Debug, Default, Clone)]
pub struct StdioProducerOptions {}

#[async_trait]
impl StreamerTrait for StdioStreamer {
    type Producer = StdioProducer;
    type Consumer = StdioConsumer;
    type ConnectOptions = StdioConnectOptions;
    type ConsumerOptions = StdioConsumerOptions;
    type ProducerOptions = StdioProducerOptions;

    async fn connect(_: StreamerUri, _: Self::ConnectOptions) -> StreamResult<Self> {
        Ok(StdioStreamer {})
    }

    fn create_generic_producer(&self, _: Self::ProducerOptions) -> StreamResult<Self::Producer> {
        Ok(StdioProducer::new())
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        _: Self::ConsumerOptions,
    ) -> StreamResult<Self::Consumer> {
        Ok(create_consumer(streams.to_vec()))
    }
}

impl ConnectOptionsTrait for StdioConnectOptions {
    fn timeout(&self) -> StreamResult<Duration> {
        Ok(Duration::from_secs(0))
    }
    fn set_timeout(&mut self, _: Duration) -> StreamResult<()> {
        Ok(())
    }
}

impl ConsumerOptionsTrait for StdioConsumerOptions {
    fn new(_: ConsumerMode) -> Self {
        Self { group: None }
    }

    fn consumer_group(&self) -> StreamResult<&ConsumerGroup> {
        self.group.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    fn set_consumer_group(&mut self, group: ConsumerGroup) -> StreamResult<()> {
        self.group = Some(group);
        Ok(())
    }
}

impl ProducerOptionsTrait for StdioProducerOptions {}
