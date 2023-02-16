<div align="center">

  <img src="https://raw.githubusercontent.com/SeaQL/sea-orm/master/docs/SeaQL logo dual.png" width="280"/>

  <h1>SeaStreamer</h1>

  <p>
    <strong>🌊 The stream processing toolkit for Rust</strong>
  </p>

  [![crate](https://img.shields.io/crates/v/sea-streamer.svg)](https://crates.io/crates/sea-streamer)
  [![docs](https://docs.rs/sea-streamer/badge.svg)](https://docs.rs/sea-streamer)
  [![build status](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml/badge.svg)](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml)

</div>

## Introduction

SeaStreamer is a stream processing toolkit to help you build stream processors in Rust.

We provide integration for Kafka / Redpanda behind a generic trait interface, so your program can be backend-agnostic.

SeaStreamer also provides a set of tools to work with streams via unix pipe, so it is testable without setting up a cluster,
and extremely handy when working with a local data set.

The API is async, and it works on `tokio` (and `actix`) and `async-std`.
