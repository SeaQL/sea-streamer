//! <div align="center">
//!
//!   <img src="./docs/SeaStreamer Banner.png"/>
//!
//!   <h1>SeaStreamer</h1>
//!
//!   <p>
//!     <strong>🌊 The stream processing toolkit for Rust</strong>
//!   </p>
//!
//!   [![crate](https://img.shields.io/crates/v/sea-streamer.svg)](https://crates.io/crates/sea-streamer)
//!   [![docs](https://docs.rs/sea-streamer/badge.svg)](https://docs.rs/sea-streamer)
//!   [![build status](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml/badge.svg)](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml)
//!
//! </div>
//!
//! SeaStreamer is a stream processing toolkit to help you build stream processors in Rust.
//!
//! ## Features
//!
//! 1. Async
//!
//! SeaStreamer provides an async API, and it supports both `tokio` and `async-std`. In tandem with other async Rust libraries,
//! you can build highly concurrent stream processors.
//!
//! 2. Generic
//!
//! We provide integration for Kafka / Redpanda behind a generic trait interface, so your program can be backend-agnostic.
//! Support for Redis Stream is being planned.
//!
//! 3. Testable
//!
//! SeaStreamer also provides a set of tools to work with streams via unix pipes, so it is testable without setting up a cluster,
//! and extremely handy when working locally.
//!
//! 4. Micro-service Oriented
//!
//! Let's build real-time (multi-threaded, no GC), self-contained (aka easy to deploy), low-resource-usage, long-running (no memory leak) stream processors in Rust!
//!
//! ## Quick Start
//!
//! Here is a basic stream consumer, [full example](https://github.com/SeaQL/sea-streamer/tree/main/examples/src/bin/consumer.rs):
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     env_logger::init();
//!
//!     let Args { stream } = Args::from_args();
//!
//!     let streamer = SeaStreamer::connect(
//!         std::env::var("STREAMER_URL")
//!             .unwrap_or_else(|_| "kafka://localhost:9092".to_owned())
//!             .parse()?,
//!         Default::default(),
//!     )
//!     .await?;
//!
//!     let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
//!     options.set_kafka_consumer_options(|options| {
//!         options.set_auto_offset_reset(AutoOffsetReset::Earliest);
//!     });
//!     let consumer: SeaConsumer = streamer.create_consumer(&[stream], options).await?;
//!
//!     loop {
//!         let mess: SeaMessage = consumer.next().await?;
//!         println!("[{}] {}", mess.timestamp(), mess.message().as_str()?);
//!     }
//! }
//! ```
//!
//! Here is a basic stream producer, [full example](https://github.com/SeaQL/sea-streamer/tree/main/examples/src/bin/producer.rs):
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     env_logger::init();
//!
//!     let Args { stream } = Args::from_args();
//!
//!     let streamer = SeaStreamer::connect(
//!         std::env::var("STREAMER_URL")
//!             .unwrap_or_else(|_| "kafka://localhost:9092".to_owned())
//!             .parse()?,
//!         Default::default(),
//!     )
//!     .await?;
//!
//!     let producer: SeaProducer = streamer.create_producer(stream, Default::default()).await?;
//!
//!     for tick in 0..10 {
//!         let message = format!(r#""tick {tick}""#);
//!         println!("{message}");
//!         producer.send(message)?;
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!     }
//!
//!     producer.flush(Duration::from_secs(10)).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! `sea-streamer` is the facade crate re-exporting implementation from a number of sub-crates:
//!
//! [`sea-streamer` API Docs](https://docs.rs/sea-streamer)
//! + `sea-streamer-types`
//! + `sea-streamer-socket`
//!     + `sea-streamer-kafka`
//!     + `sea-streamer-stdio`

#![cfg_attr(docsrs, feature(doc_cfg))]

pub use sea_streamer_types::*;

#[cfg(feature = "sea-streamer-kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-kafka")))]
pub use sea_streamer_kafka as kafka;

#[cfg(feature = "sea-streamer-stdio")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-stdio")))]
pub use sea_streamer_stdio as stdio;

#[cfg(feature = "sea-streamer-socket")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-socket")))]
pub use sea_streamer_socket::*;
