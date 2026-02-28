//! ### `sea-streamer-kafka`: Kafka / Redpanda Backend
//!
//! This is the Kafka / Redpanda backend implementation for SeaStreamer.
//! This crate provides a comprehensive type system that makes working with Kafka easier and safer.
//!
//! First of all, all API (many are sync) are properly wrapped as async. Methods are also marked `&mut` to eliminate possible race conditions.
//!
//! `KafkaConsumerOptions` has typed parameters.
//!
//! `KafkaConsumer` allows you to `seek` to point in time, `rewind` to particular offset, and `commit` message read.
//!
//! `KafkaProducer` allows you to `await` a send `Receipt` or discard it if you are uninterested. You can also flush the Producer.
//!
//! `KafkaStreamer` allows you to flush all producers on `disconnect`.
//!
//! See [tests](https://github.com/SeaQL/sea-streamer/blob/main/sea-streamer-kafka/tests/consumer.rs) for an illustration of the stream semantics.
//!
//! This crate depends on [`rdkafka`](https://docs.rs/rdkafka),
//! which in turn depends on [librdkafka-sys](https://docs.rs/librdkafka-sys), which itself is a wrapper of
//! [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/index.html).
//!
//! Configuration Reference: <https://kafka.apache.org/documentation/#configuration>

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/SeaQL/sea-streamer/main/docs/SeaQL icon.png"
)]

/// The default Kafka port number
pub const KAFKA_PORT: u16 = 9092;

/// The default timeout, if needed but unspecified
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[cfg(all(feature = "runtime-smol", feature = "runtime-tokio"))]
compile_error!("'runtime-smol' and 'runtime-tokio' cannot be enabled at the same time");

mod cluster;
mod consumer;
mod error;
mod host;
mod producer;
mod runtime;
mod streamer;

use cluster::*;
pub use consumer::*;
pub use error::*;
pub use host::*;
pub use producer::*;
pub use runtime::*;
pub use streamer::*;

/// Re-export types from `rdkafka`
pub mod export {
    pub use rdkafka;
}

macro_rules! impl_into_string {
    ($name:ident) => {
        impl From<$name> for String {
            fn from(o: $name) -> Self {
                o.as_str().to_owned()
            }
        }
    };
}

pub(crate) use impl_into_string;
