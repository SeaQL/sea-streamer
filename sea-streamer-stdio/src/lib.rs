//! ### `sea-streamer-stdio`: Standard I/O Backend
//!
//! This is the `stdio` backend implementation for SeaStreamer. It is designed to be connected together with unix pipes,
//! enabling great flexibility when developing stream processors or processing data locally.
//!
//! You can connect processors together with pipes: `processor_a | processor_b`.
//!
//! You can also connect them asynchronously:
//!
//! ```shell
//! touch stream # set up an empty file
//! tail -f stream | processor_b # program b can be spawned anytime
//! processor_a >> stream # append to the file
//! ```
//!
//! You can also use `cat` to replay a file, but it runs from start to end as fast as possible then stops,
//! which may or may not be the desired behavior.
//!
//! You can write any valid UTF-8 string to stdin and each line will be considered a message. In addition, you can write some message meta in a simple format:
//!
//! ```log
//! [timestamp | stream_key | sequence | shard_id] payload
//! ```
//!
//! Note: the square brackets are literal `[` `]`.
//!
//! The following are all valid:
//!
//! ```log
//! a plain, raw message
//! [2022-01-01T00:00:00] { "payload": "anything" }
//! [2022-01-01T00:00:00.123 | my_topic] "a string payload"
//! [2022-01-01T00:00:00 | my-topic-2 | 123] ["array", "of", "values"]
//! [2022-01-01T00:00:00 | my-topic-2 | 123 | 4] { "payload": "anything" }
//! [my_topic] a string payload
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
//!
//! Consumers in the same `ConsumerGroup` will be load balanced (in a round-robin fashion), meaning you can spawn multiple async tasks to process messages in parallel.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/SeaQL/sea-streamer/main/docs/SeaQL icon.png"
)]

/// Default stream key
pub const BROADCAST: &str = "broadcast";

use time::{format_description::FormatItem, macros::format_description};

/// Canonical time format
pub const TIMESTAMP_FORMAT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
// have no idea to how to make subsecond optional
/// Canonical time format with sub-seconds
pub const TIMESTAMP_FORMAT_SUBSEC: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");

mod consumer;
mod consumer_group;
mod error;
pub(crate) mod parser;
mod producer;
mod streamer;
mod util;

pub use consumer::*;
pub use error::*;
pub(crate) use parser::*;
pub use producer::*;
pub use streamer::*;
