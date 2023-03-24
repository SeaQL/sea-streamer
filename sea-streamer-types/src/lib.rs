//! ### `sea-streamer-types`: Traits & Types
//!
//! This crate defines all the traits and types for the SeaStreamer API, but does not provide any implementation.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/SeaQL/sea-streamer/main/docs/SeaQL icon.png"
)]

mod consumer;
mod error;
mod message;
mod options;
mod producer;
mod stream;
mod streamer;

pub use consumer::*;
pub use error::*;
pub use message::*;
pub use options::*;
pub use producer::*;
pub use stream::*;
pub use streamer::*;

/// Re-export types from related libraries
pub mod export;
