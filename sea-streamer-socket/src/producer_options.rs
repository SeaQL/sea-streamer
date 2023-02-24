use sea_streamer_kafka::KafkaProducerOptions;
use sea_streamer_stdio::StdioProducerOptions;

/// `sea-streamer-socket` concrete type of ProducerOptions.
pub type SeaProducerOptions = StdioProducerOptions;

/// `sea-streamer-socket` trait to convert between producer options of different backends.
pub trait SeaProducerOptionsTrait {
    fn into_stdio_producer_options(self) -> StdioProducerOptions;
    fn into_kafka_producer_options(self) -> KafkaProducerOptions;
}

impl SeaProducerOptionsTrait for SeaProducerOptions {
    fn into_stdio_producer_options(self) -> StdioProducerOptions {
        self
    }

    fn into_kafka_producer_options(self) -> KafkaProducerOptions {
        KafkaProducerOptions::default()
    }
}
