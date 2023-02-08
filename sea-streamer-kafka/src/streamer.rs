use async_trait::async_trait;
use rdkafka::ClientConfig;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use sea_streamer::{
    runtime_error, ConnectOptions, ConsumerGroup, ConsumerMode, StreamErr, StreamKey, Streamer,
    StreamerUri,
};
use sea_streamer_runtime::spawn_blocking;

use crate::{
    create_consumer, create_producer, host_id, impl_into_string, KafkaConsumer,
    KafkaConsumerOptions, KafkaErr, KafkaProducer, KafkaProducerOptions, KafkaResult,
};

#[derive(Debug, Clone)]
pub struct KafkaStreamer {
    uri: StreamerUri,
    producers: Arc<Mutex<Vec<KafkaProducer>>>,
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
    type Error = KafkaErr;

    fn timeout(&self) -> KafkaResult<Duration> {
        self.timeout.ok_or(StreamErr::TimeoutNotSet)
    }

    /// Timeout for network requests. Default is 1 min (as of librdkafka 3.2.1)
    fn set_timeout(&mut self, v: Duration) -> KafkaResult<&mut Self> {
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
    type Error = KafkaErr;
    type Producer = KafkaProducer;
    type Consumer = KafkaConsumer;
    type ConnectOptions = KafkaConnectOptions;
    type ConsumerOptions = KafkaConsumerOptions;
    type ProducerOptions = KafkaProducerOptions;

    /// Nothing will happen until you create a producer/consumer
    async fn connect(uri: StreamerUri, options: Self::ConnectOptions) -> KafkaResult<Self> {
        Ok(KafkaStreamer {
            uri,
            producers: Arc::new(Mutex::new(Vec::new())),
            options,
        })
    }

    /// It will flush all producers
    async fn disconnect(self) -> KafkaResult<()> {
        let producers: Vec<KafkaProducer> = {
            let mut mutex = self.producers.lock().expect("Failed to lock KafkaStreamer");
            mutex.drain(..).collect()
        };
        for producer in producers {
            spawn_blocking(move || producer.flush(std::time::Duration::from_secs(60)))
                .await
                .map_err(runtime_error)??;
        }
        Ok(())
    }

    async fn create_generic_producer(
        &self,
        _: Self::ProducerOptions,
    ) -> KafkaResult<Self::Producer> {
        let producer = create_producer(&self.uri).map_err(StreamErr::Backend)?;
        {
            let mut producers = self.producers.lock().expect("Failed to lock KafkaStreamer");
            producers.push(producer.clone());
        }
        Ok(producer)
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        mut options: Self::ConsumerOptions,
    ) -> KafkaResult<Self::Consumer> {
        if streams.is_empty() {
            return Err(StreamErr::StreamKeyEmpty);
        }
        match options.mode {
            ConsumerMode::RealTime => {
                if options.group_id.is_some() {
                    return Err(StreamErr::ConsumerGroupIsSet);
                }
                options.set_group_id(ConsumerGroup::new(format!("{}s", host_id())));
                options.set_session_timeout(std::time::Duration::from_secs(6)); // trying to set it as low as allowed
                options.set_enable_auto_commit(false);
            }
            ConsumerMode::Resumable => {
                if options.group_id.is_some() {
                    return Err(StreamErr::ConsumerGroupIsSet);
                }
                options.set_group_id(ConsumerGroup::new(format!("{}r", host_id())));
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
                .map_err(StreamErr::Backend)?,
        )
    }
}
