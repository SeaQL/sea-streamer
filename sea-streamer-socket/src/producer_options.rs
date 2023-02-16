use sea_streamer_kafka::KafkaProducerOptions;
use sea_streamer_stdio::StdioProducerOptions;

pub type SeaProducerOptions = StdioProducerOptions;

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
