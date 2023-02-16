//! # 🌊 SeaStreamer Kafka / Redpanda Backend
//!
//! This is the Kafka / Redpanda backend implementation for SeaStreamer. Although the crate's name is `kafka`,
//! Redpanda integration is first-class as well. This crate depends on [`rdkafka`](https://docs.rs/rdkafka),
//! which in turn depends on [librdkafka-sys](https://docs.rs/librdkafka-sys), which itself is a wrapper of
//! [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/index.html).
//!
//! This crate provides a comprehensive type system that makes working with Kafka easier and safer.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]

/// The default Kafka port number
pub const KAFKA_PORT: u16 = 9092;

mod cluster;
mod consumer;
mod error;
mod host;
mod producer;
mod streamer;

use cluster::*;
pub use consumer::*;
pub use error::*;
pub use host::*;
pub use producer::*;
pub use streamer::*;

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
