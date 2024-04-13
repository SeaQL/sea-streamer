//! <div align="center">
//!
//!   <img src="https://raw.githubusercontent.com/SeaQL/sea-streamer/master/docs/SeaStreamer Banner.png"/>
//!
//!   <h1>SeaStreamer</h1>
//!
//!   <p>
//!     <strong>🌊 A real-time stream processing toolkit for Rust</strong>
//!   </p>
//!
//!   [![crate](https://img.shields.io/crates/v/sea-streamer.svg)](https://crates.io/crates/sea-streamer)
//!   [![docs](https://docs.rs/sea-streamer/badge.svg)](https://docs.rs/sea-streamer)
//!   [![build status](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml/badge.svg)](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml)
//!
//! </div>
//!
//! SeaStreamer is a toolkit to help you build real-time stream processors in Rust.
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
//! We provide integration for Redis & Kafka / Redpanda behind a generic trait interface, so your program can be backend-agnostic.
//!
//! 3. Testable
//!
//! SeaStreamer also provides a set of tools to work with streams via unix pipes, so it is testable without setting up a cluster,
//! and extremely handy when working locally.
//!
//! 4. Micro-service Oriented
//!
//! Let's build real-time (multi-threaded, no GC), self-contained (aka easy to deploy), low-resource-usage, long-running stream processors in Rust!
//!
//! ## Quick Start
//!
//! Add the following to your `Cargo.toml`
//!
//! ```toml
//! sea-streamer = { version = "0", features = ["kafka", "redis", "stdio", "socket", "runtime-tokio"] }
//! ```
//!
//! Here is a basic [stream consumer](https://github.com/SeaQL/sea-streamer/tree/main/examples/src/bin/consumer.rs):
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     env_logger::init();
//!
//!     let Args { stream } = Args::parse();
//!
//!     let streamer = SeaStreamer::connect(stream.streamer(), Default::default()).await?;
//!
//!     let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
//!     options.set_auto_stream_reset(SeaStreamReset::Earliest);
//!
//!     let consumer: SeaConsumer = streamer
//!         .create_consumer(stream.stream_keys(), options)
//!         .await?;
//!
//!     loop {
//!         let mess: SeaMessage = consumer.next().await?;
//!         println!("[{}] {}", mess.timestamp(), mess.message().as_str()?);
//!     }
//! }
//! ```
//!
//! Here is a basic [stream producer](https://github.com/SeaQL/sea-streamer/tree/main/examples/src/bin/producer.rs):
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     env_logger::init();
//!
//!     let Args { stream } = Args::parse();
//!
//!     let streamer = SeaStreamer::connect(stream.streamer(), Default::default()).await?;
//!
//!     let producer: SeaProducer = streamer
//!         .create_producer(stream.stream_key()?, Default::default())
//!         .await?;
//!
//!     for tick in 0..100 {
//!         let message = format!(r#""tick {tick}""#);
//!         eprintln!("{message}");
//!         producer.send(message)?;
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!     }
//!
//!     producer.end().await?; // flush
//!
//!     Ok(())
//! }
//! ```
//!
//! Here is a [basic stream processor](https://github.com/SeaQL/sea-streamer/tree/main/examples/src/bin/processor.rs).
//! See also other [advanced stream processors](https://github.com/SeaQL/sea-streamer/tree/main/examples/).
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     env_logger::init();
//!
//!     let Args { input, output } = Args::parse();
//!
//!     let streamer = SeaStreamer::connect(input.streamer(), Default::default()).await?;
//!     let options = SeaConsumerOptions::new(ConsumerMode::RealTime);
//!     let consumer: SeaConsumer = streamer
//!         .create_consumer(input.stream_keys(), options)
//!         .await?;
//!
//!     let streamer = SeaStreamer::connect(output.streamer(), Default::default()).await?;
//!     let producer: SeaProducer = streamer
//!         .create_producer(output.stream_key()?, Default::default())
//!         .await?;
//!
//!     loop {
//!         let message: SeaMessage = consumer.next().await?;
//!         let message = process(message).await?;
//!         eprintln!("{message}");
//!         producer.send(message)?; // send is non-blocking
//!     }
//! }
//! ```
//!
//! Now, let's put them into action.
//!
//! With Redis / Kafka:
//!
//! ```shell
//! STREAMER_URI="redis://localhost:6379" # or
//! STREAMER_URI="kafka://localhost:9092"
//!
//! # Produce some input
//! cargo run --bin producer -- --stream $STREAMER_URI/hello1 &
//! # Start the processor, producing some output
//! cargo run --bin processor -- --input $STREAMER_URI/hello1 --output $STREAMER_URI/hello2 &
//! # Replay the output
//! cargo run --bin consumer -- --stream $STREAMER_URI/hello2
//! # Remember to stop the processes
//! kill %1 %2
//! ```
//!
//! With Stdio:
//!
//! ```shell
//! # Pipe the producer to the processor
//! cargo run --bin producer -- --stream stdio:///hello1 | \
//! cargo run --bin processor -- --input stdio:///hello1 --output stdio:///hello2
//! ```
//!
//! ## Architecture
//!
//! The architecture of [`sea-streamer`](https://docs.rs/sea-streamer) is constructed by a number of sub-crates:
//!
//! + [`sea-streamer-types`](https://docs.rs/sea-streamer-types)
//! + [`sea-streamer-socket`](https://docs.rs/sea-streamer-socket)
//!     + [`sea-streamer-kafka`](https://docs.rs/sea-streamer-kafka)
//!     + [`sea-streamer-redis`](https://docs.rs/sea-streamer-redis)
//!     + [`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio)
//!     + [`sea-streamer-file`](https://docs.rs/sea-streamer-file)
//! + [`sea-streamer-runtime`](https://docs.rs/sea-streamer-runtime)
//!
//! All crates share the same major version. So `0.1` of `sea-streamer` depends on `0.1` of `sea-streamer-socket`.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/SeaQL/sea-streamer/main/docs/SeaQL icon.png"
)]

pub use sea_streamer_types::*;

#[cfg(feature = "sea-streamer-kafka")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-kafka")))]
pub use sea_streamer_kafka as kafka;

#[cfg(feature = "sea-streamer-redis")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-redis")))]
pub use sea_streamer_redis as redis;

#[cfg(feature = "sea-streamer-stdio")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-stdio")))]
pub use sea_streamer_stdio as stdio;

#[cfg(feature = "sea-streamer-file")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-file")))]
pub use sea_streamer_file as file;

#[cfg(feature = "sea-streamer-socket")]
#[cfg_attr(docsrs, doc(cfg(feature = "sea-streamer-socket")))]
pub use sea_streamer_socket::*;

#[cfg(any(
    feature = "runtime",
    feature = "runtime-tokio",
    feature = "runtime-async-std"
))]
pub use sea_streamer_runtime as runtime;
