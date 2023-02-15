//! # 🌊 SeaStreamer standard I/O Backend
//!
//! This is the `stdio` backend implementation for SeaStreamer. It is designed to be connected together with unix pipes,
//! enabling great flexibility developing stream processors or processing data locally.
//!
//! You can write any valid UTF-8 string to stdin and each line will be considered a message.
//!
//! In addition, you can write some message meta with a simple format:
//!
//! ```log
//! [timestamp | stream key | sequence | shard_id] payload
//! ```
//!
//! Note: the square brackets are literal `[]`.
//!
//! The following are all valid:
//!
//! ```log
//! a plain, raw message
//! [2022-01-01T00:00:00] { "payload": "anything" }
//! [2022-01-01T00:00:00.123 | my_topic] "A string payload"
//! [2022-01-01T00:00:00 | my-topic-2 | 123] ["array", "of", "values"]
//! [2022-01-01T00:00:00 | my-topic-2 | 123 | 4] { "payload": "anything" }
//! [my_topic] "A string payload"
//! [my_topic | 123] { "payload": "anything" }
//! [my_topic | 123 | 4] { "payload": "anything" }
//! ```
//!
//! The following are all invalid:
//!
//! ```log
//! [Jan 1, 2022] { "payload": "anything" }
//! [2022-01-01T00:00:00] 12345
//! ```
//!
//! If no stream key is given, it will be assigned the name `broadcast` and sent to all consumers.
//!
//! You can create consumers that subscribe to only a subset of the topics.
//! Consumers in the same `ConsumerGroup` will be load balanced, meaning you can spawn multiple async tasks to process messages in parallel.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]

mod consumers;
mod error;
pub(crate) mod parser;
mod producer;
mod streamer;
mod util;

pub use consumers::*;
pub use error::*;
pub use producer::*;
pub use streamer::*;
