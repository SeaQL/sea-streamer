pub use rdkafka::error::KafkaError as KafkaErr;
use sea_streamer::StreamResult;

pub type KafkaResult<T> = StreamResult<T, KafkaErr>;
