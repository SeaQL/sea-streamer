//! ### `sea-streamer-runtime`: Async runtime abstraction
//!
//! This crate provides a small set of functions aligning the type signatures between `smol` and `tokio`,
//! so that you can build applications generic to both runtimes.

#[cfg(all(feature = "runtime-smol", feature = "runtime-tokio"))]
compile_error!("'runtime-smol' and 'runtime-tokio' cannot be enabled at the same time");

#[cfg(feature = "file")]
pub mod file;
mod mutex;
mod sleep;
mod task;
mod timeout;

pub use mutex::*;
pub use sleep::*;
pub use task::*;
pub use timeout::*;
