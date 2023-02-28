use sea_streamer_kafka::KafkaProducerOptions;
use sea_streamer_stdio::StdioProducerOptions;
use sea_streamer_types::ProducerOptions;

#[derive(Debug, Default, Clone)]
/// `sea-streamer-socket` concrete type of ProducerOptions.
pub struct SeaProducerOptions {
    stdio: StdioProducerOptions,
    kafka: KafkaProducerOptions,
}

impl SeaProducerOptions {
    pub fn into_stdio_producer_options(self) -> StdioProducerOptions {
        self.stdio
    }

    pub fn into_kafka_producer_options(self) -> KafkaProducerOptions {
        self.kafka
    }

    /// Set options that only applies to Kafka
    pub fn set_kafka_producer_options<F: FnOnce(&mut KafkaProducerOptions)>(&mut self, func: F) {
        func(&mut self.kafka)
    }
}

impl ProducerOptions for SeaProducerOptions {}
