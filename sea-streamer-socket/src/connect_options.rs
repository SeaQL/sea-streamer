#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::KafkaConnectOptions;
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::RedisConnectOptions;
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioConnectOptions;

use crate::{map_err, BackendErr, SeaResult};
use sea_streamer_types::ConnectOptions;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
/// `sea-streamer-socket` concrete type of ConnectOptions.
pub struct SeaConnectOptions {
    #[cfg(feature = "backend-stdio")]
    stdio: StdioConnectOptions,
    #[cfg(feature = "backend-kafka")]
    kafka: KafkaConnectOptions,
    #[cfg(feature = "backend-redis")]
    redis: RedisConnectOptions,
}

impl SeaConnectOptions {
    #[cfg(feature = "backend-stdio")]
    pub fn into_stdio_connect_options(self) -> StdioConnectOptions {
        self.stdio
    }

    #[cfg(feature = "backend-kafka")]
    pub fn into_kafka_connect_options(self) -> KafkaConnectOptions {
        self.kafka
    }

    #[cfg(feature = "backend-redis")]
    pub fn into_redis_connect_options(self) -> RedisConnectOptions {
        self.redis
    }

    #[cfg(feature = "backend-stdio")]
    /// Set options that only applies to Stdio
    pub fn set_stdio_connect_options<F: FnOnce(&mut StdioConnectOptions)>(&mut self, func: F) {
        func(&mut self.stdio)
    }

    #[cfg(feature = "backend-kafka")]
    /// Set options that only applies to Kafka
    pub fn set_kafka_connect_options<F: FnOnce(&mut KafkaConnectOptions)>(&mut self, func: F) {
        func(&mut self.kafka)
    }

    #[cfg(feature = "backend-redis")]
    /// Set options that only applies to Redis
    pub fn set_redis_connect_options<F: FnOnce(&mut RedisConnectOptions)>(&mut self, func: F) {
        func(&mut self.redis)
    }
}

impl ConnectOptions for SeaConnectOptions {
    type Error = BackendErr;

    fn timeout(&self) -> SeaResult<Duration> {
        self.stdio.timeout().map_err(map_err)
    }

    fn set_timeout(&mut self, d: Duration) -> SeaResult<&mut Self> {
        #[cfg(feature = "backend-stdio")]
        self.stdio.set_timeout(d).map_err(map_err)?;
        #[cfg(feature = "backend-kafka")]
        self.kafka.set_timeout(d).map_err(map_err)?;
        #[cfg(feature = "backend-redis")]
        self.redis.set_timeout(d).map_err(map_err)?;

        Ok(self)
    }
}
