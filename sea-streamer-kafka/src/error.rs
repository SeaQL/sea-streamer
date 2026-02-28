/// Re-export of [`rdkafka::error::KafkaError`].
pub use rdkafka::error::KafkaError as KafkaErr;
use sea_streamer_types::{StreamErr, StreamResult};

/// Alias for `StreamResult<T, KafkaErr>`.
pub type KafkaResult<T> = StreamResult<T, KafkaErr>;

pub(crate) fn stream_err(err: KafkaErr) -> StreamErr<KafkaErr> {
    StreamErr::Backend(err)
}
