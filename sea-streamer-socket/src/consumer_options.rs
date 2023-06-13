#[cfg(feature = "backend-file")]
use sea_streamer_file::{AutoStreamReset as FileAutoStreamReset, FileConsumerOptions};
#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::{AutoOffsetReset, KafkaConsumerOptions};
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::{AutoStreamReset as RedisAutoStreamReset, RedisConsumerOptions};
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioConsumerOptions;

use crate::{map_err, BackendErr, SeaResult};
use sea_streamer_types::{ConsumerGroup, ConsumerMode, ConsumerOptions};

#[derive(Debug, Default, Clone)]
/// `sea-streamer-socket` concrete type of ConsumerOptions.
pub struct SeaConsumerOptions {
    #[cfg(feature = "backend-kafka")]
    kafka: KafkaConsumerOptions,
    #[cfg(feature = "backend-redis")]
    redis: RedisConsumerOptions,
    #[cfg(feature = "backend-stdio")]
    stdio: StdioConsumerOptions,
    #[cfg(feature = "backend-file")]
    file: FileConsumerOptions,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SeaStreamReset {
    Earliest,
    Latest,
}

impl SeaConsumerOptions {
    #[cfg(feature = "backend-kafka")]
    pub fn into_kafka_consumer_options(self) -> KafkaConsumerOptions {
        self.kafka
    }

    #[cfg(feature = "backend-redis")]
    pub fn into_redis_consumer_options(self) -> RedisConsumerOptions {
        self.redis
    }

    #[cfg(feature = "backend-stdio")]
    pub fn into_stdio_consumer_options(self) -> StdioConsumerOptions {
        self.stdio
    }

    #[cfg(feature = "backend-file")]
    pub fn into_file_consumer_options(self) -> FileConsumerOptions {
        self.file
    }

    #[cfg(feature = "backend-kafka")]
    /// Set options that only applies to Kafka
    pub fn set_kafka_consumer_options<F: FnOnce(&mut KafkaConsumerOptions)>(&mut self, func: F) {
        func(&mut self.kafka)
    }

    #[cfg(feature = "backend-redis")]
    /// Set options that only applies to Redis
    pub fn set_redis_consumer_options<F: FnOnce(&mut RedisConsumerOptions)>(&mut self, func: F) {
        func(&mut self.redis)
    }

    #[cfg(feature = "backend-stdio")]
    /// Set options that only applies to Stdio
    pub fn set_stdio_consumer_options<F: FnOnce(&mut StdioConsumerOptions)>(&mut self, func: F) {
        func(&mut self.stdio)
    }

    #[cfg(feature = "backend-file")]
    /// Set options that only applies to File
    pub fn set_file_consumer_options<F: FnOnce(&mut FileConsumerOptions)>(&mut self, func: F) {
        func(&mut self.file)
    }

    pub fn set_auto_stream_reset(&mut self, _val: SeaStreamReset) {
        #[cfg(feature = "backend-kafka")]
        self.set_kafka_consumer_options(|options| {
            options.set_auto_offset_reset(match _val {
                SeaStreamReset::Earliest => AutoOffsetReset::Earliest,
                SeaStreamReset::Latest => AutoOffsetReset::Latest,
            });
        });
        #[cfg(feature = "backend-redis")]
        self.set_redis_consumer_options(|options| {
            options.set_auto_stream_reset(match _val {
                SeaStreamReset::Earliest => RedisAutoStreamReset::Earliest,
                SeaStreamReset::Latest => RedisAutoStreamReset::Latest,
            });
        });
        #[cfg(feature = "backend-file")]
        self.set_file_consumer_options(|options| {
            options.set_auto_stream_reset(match _val {
                SeaStreamReset::Earliest => FileAutoStreamReset::Earliest,
                SeaStreamReset::Latest => FileAutoStreamReset::Latest,
            });
        });
    }
}

impl ConsumerOptions for SeaConsumerOptions {
    type Error = BackendErr;

    fn new(mode: ConsumerMode) -> Self {
        Self {
            #[cfg(feature = "backend-kafka")]
            kafka: KafkaConsumerOptions::new(mode),
            #[cfg(feature = "backend-redis")]
            redis: RedisConsumerOptions::new(mode),
            #[cfg(feature = "backend-stdio")]
            stdio: StdioConsumerOptions::new(mode),
            #[cfg(feature = "backend-file")]
            file: FileConsumerOptions::new(mode),
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
        #![allow(clippy::redundant_clone)]
        #[cfg(feature = "backend-kafka")]
        self.kafka
            .set_consumer_group(group_id.clone())
            .map_err(map_err)?;
        #[cfg(feature = "backend-redis")]
        self.redis
            .set_consumer_group(group_id.clone())
            .map_err(map_err)?;
        #[cfg(feature = "backend-stdio")]
        self.stdio
            .set_consumer_group(group_id.clone())
            .map_err(map_err)?;
        #[cfg(feature = "backend-file")]
        self.file
            .set_consumer_group(group_id.clone())
            .map_err(map_err)?;
        Ok(self)
    }
}
