use std::time::Duration;

use sea_streamer_kafka::KafkaConnectOptions;
use sea_streamer_stdio::StdioConnectOptions;
use sea_streamer_types::ConnectOptions;

use crate::{map_err, BackendErr, SeaResult};

#[derive(Debug, Default, Clone)]
/// `sea-streamer-socket` concrete type of ConnectOptions.
pub struct SeaConnectOptions {
    stdio: StdioConnectOptions,
    kafka: KafkaConnectOptions,
}

impl SeaConnectOptions {
    pub fn into_stdio_connect_options(self) -> StdioConnectOptions {
        self.stdio
    }

    pub fn into_kafka_connect_options(self) -> KafkaConnectOptions {
        self.kafka
    }

    /// Set options that only applies to Stdio
    pub fn set_stdio_connect_options<F: FnOnce(&mut StdioConnectOptions)>(&mut self, func: F) {
        func(&mut self.stdio)
    }

    /// Set options that only applies to Kafka
    pub fn set_kafka_connect_options<F: FnOnce(&mut KafkaConnectOptions)>(&mut self, func: F) {
        func(&mut self.kafka)
    }
}

impl ConnectOptions for SeaConnectOptions {
    type Error = BackendErr;

    fn timeout(&self) -> SeaResult<Duration> {
        self.stdio.timeout().map_err(map_err)
    }

    fn set_timeout(&mut self, d: Duration) -> SeaResult<&mut Self> {
        self.stdio.set_timeout(d).map_err(map_err)?;
        self.kafka.set_timeout(d).map_err(map_err)?;
        Ok(self)
    }
}
