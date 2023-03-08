//! ### `sea-streamer-redis`: Redis Backend

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]

/// The default Redis port number
pub const REDIS_PORT: u16 = 6379;

/// The field of the message payload
pub const MSG: &str = "msg";

use sea_streamer_types::ShardId;
/// Shard 0
pub const ZERO: ShardId = ShardId::new(0);

#[cfg(all(feature = "runtime-async-std", feature = "runtime-tokio"))]
compile_error!("'runtime-async-std' and 'runtime-tokio' cannot be enabled at the same time");

mod error;
mod message;

pub use error::*;
pub use message::*;
