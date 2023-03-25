#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::KafkaProducerOptions;
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::RedisProducerOptions;
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioProducerOptions;

use sea_streamer_types::ProducerOptions;

#[derive(Debug, Default, Clone)]
/// `sea-streamer-socket` concrete type of ProducerOptions.
pub struct SeaProducerOptions {
    #[cfg(feature = "backend-stdio")]
    stdio: StdioProducerOptions,
    #[cfg(feature = "backend-kafka")]
    kafka: KafkaProducerOptions,
    #[cfg(feature = "backend-redis")]
    redis: RedisProducerOptions,
}

impl SeaProducerOptions {
    #[cfg(feature = "backend-stdio")]
    pub fn into_stdio_producer_options(self) -> StdioProducerOptions {
        self.stdio
    }

    #[cfg(feature = "backend-kafka")]
    pub fn into_kafka_producer_options(self) -> KafkaProducerOptions {
        self.kafka
    }

    #[cfg(feature = "backend-redis")]
    pub fn into_redis_producer_options(self) -> RedisProducerOptions {
        self.redis
    }

    #[cfg(feature = "backend-stdio")]
    /// Set options that only applies to Stdio
    pub fn set_stdio_producer_options<F: FnOnce(&mut StdioProducerOptions)>(&mut self, func: F) {
        func(&mut self.stdio)
    }

    #[cfg(feature = "backend-kafka")]
    /// Set options that only applies to Kafka
    pub fn set_kafka_producer_options<F: FnOnce(&mut KafkaProducerOptions)>(&mut self, func: F) {
        func(&mut self.kafka)
    }

    #[cfg(feature = "backend-redis")]
    /// Set options that only applies to Redis
    pub fn set_redis_producer_options<F: FnOnce(&mut RedisProducerOptions)>(&mut self, func: F) {
        func(&mut self.redis)
    }
}

impl ProducerOptions for SeaProducerOptions {}
