use async_trait::async_trait;
use std::time::Duration;

use crate::{
    consumers, create_consumer, producer, StdioConsumer, StdioErr, StdioProducer, StdioResult,
};
use sea_streamer::{
    ConnectOptions as ConnectOptionsTrait, ConsumerGroup, ConsumerMode,
    ConsumerOptions as ConsumerOptionsTrait, ProducerOptions as ProducerOptionsTrait, StreamErr,
    StreamKey, Streamer as StreamerTrait, StreamerUri,
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
    type Error = StdioErr;
    type Producer = StdioProducer;
    type Consumer = StdioConsumer;
    type ConnectOptions = StdioConnectOptions;
    type ConsumerOptions = StdioConsumerOptions;
    type ProducerOptions = StdioProducerOptions;

    /// Nothing will happen until you create a producer/consumer
    async fn connect(_: StreamerUri, _: Self::ConnectOptions) -> StdioResult<Self> {
        Ok(StdioStreamer {})
    }

    /// Call this method if you want to exit gracefully. This waits asynchronously until the background thread(s) exit.
    /// Meanwhile pending input and output messages will be flushed.
    /// The side effects is global: all existing consumers and producers will become unusable, until you connect again.
    async fn disconnect(self) -> StdioResult<()> {
        consumers::shutdown();
        producer::shutdown();
        while !consumers::shutdown_already() || !producer::shutdown_already() {
            sea_streamer_runtime::sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    async fn create_generic_producer(
        &self,
        _: Self::ProducerOptions,
    ) -> StdioResult<Self::Producer> {
        Ok(StdioProducer::new())
    }

    /// A background thread will be spawned to read stdin dedicatedly.
    /// It is safe to spawn multiple consumers.
    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> StdioResult<Self::Consumer> {
        if streams.is_empty() {
            return Err(StreamErr::StreamKeyEmpty);
        }
        match options.mode {
            ConsumerMode::RealTime => {
                if options.group.is_some() {
                    log::warn!("Consumer group is set and thus will be load-balanced.");
                }
                Ok(create_consumer(options.group, streams.to_vec()))
            }
            ConsumerMode::Resumable => Err(StreamErr::Unsupported(
                "stdio does not support Resumable".to_owned(),
            )),
            ConsumerMode::LoadBalanced => {
                if options.group.is_some() {
                    Ok(create_consumer(options.group, streams.to_vec()))
                } else {
                    Err(StreamErr::ConsumerGroupNotSet)
                }
            }
        }
    }
}

impl ConnectOptionsTrait for StdioConnectOptions {
    type Error = StdioErr;

    fn timeout(&self) -> StdioResult<Duration> {
        Ok(Duration::from_secs(0))
    }

    /// This parameter is ignored because this should never fail
    fn set_timeout(&mut self, _: Duration) -> StdioResult<&mut Self> {
        Ok(self)
    }
}

impl ConsumerOptionsTrait for StdioConsumerOptions {
    type Error = StdioErr;

    fn new(mode: ConsumerMode) -> Self {
        Self { mode, group: None }
    }

    fn consumer_group(&self) -> StdioResult<&ConsumerGroup> {
        self.group.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    /// If multiple consumers shares the same group, only one in the group will receive a message
    /// This is load-balanced in a round-robin fashion
    fn set_consumer_group(&mut self, group: ConsumerGroup) -> StdioResult<&mut Self> {
        self.group = Some(group);
        Ok(self)
    }
}

impl Default for StdioConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

impl ProducerOptionsTrait for StdioProducerOptions {}
