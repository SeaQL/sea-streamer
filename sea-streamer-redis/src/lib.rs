//! ### `sea-streamer-redis`: Redis Backend
//!
//! > 🚧 To be released
//!
//! This is the Redis backend implementation for SeaStreamer.
//! This crate provides a high-level async API on top of Redis that makes working with Redis Streams fool-proof:
//!
//! + Implements the familiar SeaStreamer abstract interface
//! + A comprehensive type system that guides/restricts you with the API
//! + High-level API, so you don't call `XADD`, `XREAD` or `XACK` anymore
//! + Mutex-free implementation: concurrency achieved by message passing
//!
//! While we'd like to provide a Kafka-like client experience, there are some fundamental differences between Redis and Kafka:
//!
//! 1. In Redis sequence numbers are not contiguous
//! 2. In Redis messages are dispatched to consumers among group members in a first-ask-first-served manner, which leads to the next point
//! 3. In Redis `ACK` has to be done per message
//!
//! What's already implemented:
//!
//! + RealTime mode with AutoStreamReset
//! + Resumable mode with 4 `ACK` mechanisms
//! + LoadBalanced mode with failover behaviour
//! + Seek/rewind to point in time
//! + Basic stream sharding: split a stream into multiple sub-streams
//!
//! It's best to look through the [tests](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-redis/tests)
//! for an illustration of the different streaming behaviour.
//!
//! How SeaStreamer offers better concurrency?
//!
//! Consider the following simple stream processor:
//!
//! ```ignore
//! loop {
//!     let messages = XREAD.await
//!     process(messages).await
//! }
//! ```
//!
//! When it's reading, it's not processing. So it's wasting time idle and reading messages with a higher delay, which in turn limits the throughput.
//!
//! In SeaStreamer, the read loop is separate from your process loop, so they can happen in parallel.
//!
//! If you are reading from a consumer group, you also have to consider when to ACK and how many ACKs to batch in one request.
//!
//! SeaStreamer can commit in the background on a regular interval, or you can commit asynchronously without blocking your process loop.
//!
//! In the future, we'd like to support Redis Cluster, because sharding without clustering is not very useful.
//! Right now it's pretty much a work-in-progress.
//! It's quite a difficult task, because clients have to take responsibility when working with a cluster.
//! In Redis, shards and nodes is a M-N mapping - shards can be moved among nodes *at any time*.
//! It makes testing much more difficult.
//! Let us know if you'd like to help!

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/SeaQL/sea-streamer/main/docs/SeaQL icon.png"
)]

/// The default Redis port number
pub const REDIS_PORT: u16 = 6379;

/// The default timeout, if needed but unspecified
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// The field of the message payload
pub const MSG: &str = "msg";

use sea_streamer_types::ShardId;
/// Shard 0
pub const ZERO: ShardId = ShardId::new(0);

#[cfg(all(feature = "runtime-async-std", feature = "runtime-tokio"))]
compile_error!("'runtime-async-std' and 'runtime-tokio' cannot be enabled at the same time");

mod cluster;
mod connection;
mod consumer;
mod error;
mod host;
mod message;
mod producer;
mod streamer;

pub use cluster::*;
pub use connection::*;
pub use consumer::*;
pub use error::*;
pub use host::*;
pub use message::*;
pub use producer::*;
pub use streamer::*;
