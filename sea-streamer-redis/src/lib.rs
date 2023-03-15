//! ### `sea-streamer-redis`: Redis Backend
//!
//! > 🚧 Work in Progress

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]

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
