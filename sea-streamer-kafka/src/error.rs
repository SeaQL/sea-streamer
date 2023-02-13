pub use rdkafka::error::KafkaError as KafkaErr;
use sea_streamer::{StreamErr, StreamResult};

pub type KafkaResult<T> = StreamResult<T, KafkaErr>;

pub(crate) fn stream_err(err: KafkaErr) -> StreamErr<KafkaErr> {
    StreamErr::Backend(err)
}
