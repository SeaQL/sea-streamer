//! <div align="center">
//!
//!   <img src="https://raw.githubusercontent.com/SeaQL/sea-orm/master/docs/SeaQL logo dual.png" width="280"/>
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
//! ## Architecture
//!
//! `sea-streamer` is the facade crate re-exporting implementation from a number of sub-crates:
//!
//! + [sea-streamer-types](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-types)
//! + [sea-streamer-socket](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-socket)
//! + [sea-streamer-kafka](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-kafka)
//! + [sea-streamer-stdio](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-stdio)

pub use sea_streamer_types::*;

#[cfg(feature = "sea-streamer-socket")]
pub use sea_streamer_socket::*;
