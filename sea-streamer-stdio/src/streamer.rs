use async_trait::async_trait;
use std::time::Duration;

use crate::{create_consumer, shutdown, shutdown_already, StdioConsumer, StdioProducer};
use sea_streamer::{
    ConnectOptions as ConnectOptionsTrait, ConsumerGroup, ConsumerMode,
    ConsumerOptions as ConsumerOptionsTrait, ProducerOptions as ProducerOptionsTrait, StreamErr,
    StreamKey, StreamResult, Streamer as StreamerTrait, StreamerUri,
};

#[derive(Debug, Default, Clone)]
pub struct StdioStreamer {}

#[derive(Debug, Default, Clone)]
pub struct StdioConnectOptions {}

#[derive(Debug, Clone)]
pub struct StdioConsumerOptions {
    mode: ConsumerMode,
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

    async fn disconnect(self) -> StreamResult<()> {
        shutdown();
        while !shutdown_already() {
            sea_streamer_runtime::sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    async fn create_generic_producer(
        &self,
        _: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer> {
        Ok(StdioProducer::new())
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> StreamResult<Self::Consumer> {
        if options.mode != ConsumerMode::RealTime {
            return Err(StreamErr::Unsupported(
                "stdio only supports RealTime".to_owned(),
            ));
        }
        Ok(create_consumer(options.group, streams.to_vec()))
    }
}

impl ConnectOptionsTrait for StdioConnectOptions {
    fn timeout(&self) -> StreamResult<Duration> {
        Ok(Duration::from_secs(0))
    }

    /// This parameter is ignored because this should never fail
    fn set_timeout(&mut self, _: Duration) -> StreamResult<()> {
        Ok(())
    }
}

impl ConsumerOptionsTrait for StdioConsumerOptions {
    fn new(mode: ConsumerMode) -> Self {
        Self { mode, group: None }
    }

    fn consumer_group(&self) -> StreamResult<&ConsumerGroup> {
        self.group.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    /// If multiple consumers shares the same group, only one in the group will receive a message
    fn set_consumer_group(&mut self, group: ConsumerGroup) -> StreamResult<()> {
        self.group = Some(group);
        Ok(())
    }
}

impl Default for StdioConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

impl ProducerOptionsTrait for StdioProducerOptions {}
