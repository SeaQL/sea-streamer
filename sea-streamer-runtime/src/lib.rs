//! ### `sea-streamer-runtime`: Async runtime abstraction
//!
//! This crate provides a small set of functions aligning the type signatures between `async-std` and `tokio`,
//! so that you can build applications generic to both runtimes.

mod sleep;
mod task;

pub use sleep::*;
pub use task::*;
