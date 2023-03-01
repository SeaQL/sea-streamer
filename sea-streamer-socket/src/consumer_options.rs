use sea_streamer_kafka::KafkaConsumerOptions;
use sea_streamer_stdio::StdioConsumerOptions;
use sea_streamer_types::{ConsumerGroup, ConsumerMode, ConsumerOptions};

use crate::{map_err, BackendErr, SeaResult};

#[derive(Debug, Default, Clone)]
/// `sea-streamer-socket` concrete type of ConsumerOptions.
pub struct SeaConsumerOptions {
    stdio: StdioConsumerOptions,
    kafka: KafkaConsumerOptions,
}

impl SeaConsumerOptions {
    pub fn into_stdio_consumer_options(self) -> StdioConsumerOptions {
        self.stdio
    }

    pub fn into_kafka_consumer_options(self) -> KafkaConsumerOptions {
        self.kafka
    }

    /// Set options that only applies to Stdio
    pub fn set_stdio_consumer_options<F: FnOnce(&mut StdioConsumerOptions)>(&mut self, func: F) {
        func(&mut self.stdio)
    }

    /// Set options that only applies to Kafka
    pub fn set_kafka_consumer_options<F: FnOnce(&mut KafkaConsumerOptions)>(&mut self, func: F) {
        func(&mut self.kafka)
    }
}

impl ConsumerOptions for SeaConsumerOptions {
    type Error = BackendErr;

    fn new(mode: ConsumerMode) -> Self {
        Self {
            stdio: StdioConsumerOptions::new(mode),
            kafka: KafkaConsumerOptions::new(mode),
        }
    }

    /// Get currently set ConsumerMode
    fn mode(&self) -> SeaResult<&ConsumerMode> {
        self.stdio.mode().map_err(map_err)
    }

    /// Get currently set consumer group; may return `StreamErr::ConsumerGroupNotSet`.
    fn consumer_group(&self) -> SeaResult<&ConsumerGroup> {
        self.stdio.consumer_group().map_err(map_err)
    }

    /// Set consumer group for this consumer. Note the semantic is implementation-specific.
    fn set_consumer_group(&mut self, group_id: ConsumerGroup) -> SeaResult<&mut Self> {
        self.stdio
            .set_consumer_group(group_id.clone())
            .map_err(map_err)?;
        self.kafka.set_consumer_group(group_id).map_err(map_err)?;
        Ok(self)
    }
}
