use async_trait::async_trait;
use rdkafka::ClientConfig;
use std::time::Duration;

use sea_streamer::{
    ConnectOptions, ConsumerGroup, ConsumerMode, StreamErr, StreamKey, StreamResult, Streamer,
    StreamerUri,
};

use crate::{
    create_consumer, impl_into_string, random_id, AutoOffsetReset, KafkaConsumer,
    KafkaConsumerOptions, KafkaProducer, KafkaProducerOptions,
};

#[derive(Debug, Clone)]
pub struct KafkaStreamer {
    uri: StreamerUri,
    options: KafkaConnectOptions,
}

#[derive(Debug, Default, Clone)]
pub struct KafkaConnectOptions {
    timeout: Option<Duration>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OptionKey {
    SocketTimeout,
}

impl ConnectOptions for KafkaConnectOptions {
    fn timeout(&self) -> StreamResult<Duration> {
        self.timeout.ok_or(StreamErr::TimeoutNotSet)
    }

    /// Timeout for network requests. Default is 1 min (as of librdkafka 3.2.1)
    fn set_timeout(&mut self, v: Duration) -> StreamResult<&mut Self> {
        self.timeout = Some(v);
        Ok(self)
    }
}

impl KafkaConnectOptions {
    pub(crate) fn make_client_config(&self, client_config: &mut ClientConfig) {
        if let Some(v) = self.timeout {
            client_config.set(OptionKey::SocketTimeout, format!("{}", v.as_millis()));
        }
    }
}

impl OptionKey {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SocketTimeout => "socket.timeout.ms",
        }
    }
}

impl_into_string!(OptionKey);

#[async_trait]
impl Streamer for KafkaStreamer {
    type Producer = KafkaProducer;
    type Consumer = KafkaConsumer;
    type ConnectOptions = KafkaConnectOptions;
    type ConsumerOptions = KafkaConsumerOptions;
    type ProducerOptions = KafkaProducerOptions;

    /// Nothing will happen until you create a producer/consumer
    async fn connect(uri: StreamerUri, options: Self::ConnectOptions) -> StreamResult<Self> {
        Ok(KafkaStreamer { uri, options })
    }

    /// It will flush all producers
    async fn disconnect(self) -> StreamResult<()> {
        unimplemented!()
    }

    async fn create_generic_producer(
        &self,
        _: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer> {
        unimplemented!()
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        mut options: Self::ConsumerOptions,
    ) -> StreamResult<Self::Consumer> {
        match options.mode {
            ConsumerMode::RealTime => {
                if options.group_id.is_some() {
                    return Err(StreamErr::ConsumerGroupIsSet);
                }
                options.set_group_id(ConsumerGroup::new(random_id()));
                options.set_enable_auto_commit(false);
            }
            ConsumerMode::Resumable => {
                if options.group_id.is_some() {
                    return Err(StreamErr::ConsumerGroupIsSet);
                }
                options.set_group_id(ConsumerGroup::new(random_id()));
                options.set_enable_auto_commit(true);
            }
            ConsumerMode::LoadBalanced => {
                if options.group_id.is_none() {
                    return Err(StreamErr::ConsumerGroupNotSet);
                }
            }
        }

        Ok(
            create_consumer(&self.uri, &self.options, &options, streams.to_vec())
                .map_err(|e| StreamErr::Backend(Box::new(e)))?,
        )
    }
}
