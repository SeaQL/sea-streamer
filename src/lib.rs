#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]

//! <div align="center">
//!
//!   <img src="https://raw.githubusercontent.com/SeaQL/sea-streamer/master/docs/SeaQL logo dual.png" width="280"/>
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
//! ## Introduction
//!
//! SeaStreamer is a stream processing toolkit to help you build stream processors in Rust.
//!
//! We provide integration for Kafka, Redpanda and Redis Stream behind a generic trait interface, so your program can be backend-agnostic.
//!
//! SeaStreamer also provide a set of tools to work with streams via unix file and pipe, so it is testable without setting up a cluster.
//!
//! The API is async, but the facade crate makes no assumption to the async runtime.
