//! ### `sea-streamer-socket`: Backend-agnostic Socket API
//!
//! Akin to how SeaORM allows you to build applications for different databases, SeaStreamer allows you to build
//! stream processors for different streaming servers.
//!
//! While the `sea-streamer-types` crate provides a nice trait-based abstraction, this crates provides a concrete-type API,
//! so that your program can stream from/to any SeaStreamer backend selected by the user *on runtime*.
//!
//! This allows you to do neat things, like generating data locally and then stream them to Kafka. Or in the other
//! way, sink data from Kafka to work on them locally. All _without recompiling_ the stream processor.
//!
//! If you only ever work with Kafka, feel free to depend on `sea-streamer-kafka` directly.
//!
//! A small number of cli programs are provided for demonstration. Let's set them up first:
//!
//! ```shell
//! # The `clock` program generate messages in the form of `{ "tick": N }`
//! alias clock='cargo run --package sea-streamer-stdio  --features=executables --bin clock'
//! # The `relay` program redirect messages from `input` to `output`
//! alias relay='cargo run --package sea-streamer-socket --features=executables --bin relay'
//! ```
//!
//! Here is how to stream from Stdio ➡️ Kafka. We generate messages using `clock` and then pipe it to `relay`,
//! which then streams to Kafka:
//!
//! ```shell
//! clock -- --stream clock --interval 1s | \
//! relay -- --input stdio:///clock --output kafka://localhost:9092/clock
//! ```
//!
//! Here is how to *replay* the stream from Kafka ➡️ Stdio:
//!
//! ```shell
//! relay -- --input kafka://localhost:9092/clock --output stdio:///clock --offset start
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/SeaQL/sea-streamer/main/docs/SeaQL icon.png"
)]

mod backend;
mod connect_options;
mod consumer;
mod consumer_options;
mod error;
mod message;
mod producer;
mod producer_options;
mod streamer;

pub use backend::*;
pub use connect_options::*;
pub use consumer::*;
pub use consumer_options::*;
pub use error::*;
pub use message::*;
pub use producer::*;
pub use producer_options::*;
pub use streamer::*;
