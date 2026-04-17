#![deny(missing_debug_implementations)]

/// The default Apache Iggy port number
pub const IGGY_PORT: u16 = 8090;

/// The default timeout, if needed but unspecified
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

mod consumer;
mod error;
mod message;
mod options;
mod producer;
mod streamer;

pub use consumer::*;
pub use error::*;
pub use message::*;
pub use options::*;
pub use producer::*;
pub use streamer::*;

pub mod export {
    pub use iggy;
}
