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

SeaStreamer is a stream processing toolkit to help you build stream processors in Rust.

We provide integration for Kafka / Redpanda behind a generic trait interface, so your program can be backend-agnostic. Support for Redis Stream is being planned.

SeaStreamer also provides a set of tools to work with streams via unix pipe, so it is testable without setting up a cluster,
and extremely handy when working with a local data set.

The API is async, and it works on `tokio` (and `actix`) and `async-std`.

This is the facade crate re-exporting implementation from a number of sub-crates.

## `sea-streamer-types` SeaStreamer Types

This crate defines all the traits and types for the SeaStreamer API, but does not provide any implementation.

## `sea-streamer-socket` SeaStreamer backend-agnostic Socket API

Akin to how SeaORM allows you to build applications for different databases, SeaStreamer allows you to build
stream processors for any streaming server.

While the `sea-streamer-types` provides a nice trait-based abstraction, this crates provides a concrete API,
so that your program can stream to any SeaStreamer backend selected by the user *on runtime*.

This allows you to do neat things, like generating data locally and then stream them to Kafka. Or in the other
way, sink data from Kafka to work on them locally. All _without recompiling_ the stream processor.

If you only ever work with Kafka, feel free to depend on `sea-streamer-kafka` directly.

A small number of cli programs are provided for demonstration. Let's set them up first:

```sh
# The `clock` program generate messages in the form of `{ "tick": N }`
alias clock='cargo run --package sea-streamer-stdio  --bin clock --features=executables'
# The `relay` program redirect messages from `input` to `output`
alias relay='cargo run --package sea-streamer-socket --bin relay --features=executables'
```

Here is how to stream from Stdio -> Kafka. We generate messages using `clock` and then pipe it to `relay`,
which then streams to Kafka:

```sh
clock -- --interval 1s --stream clock | \
relay -- --input stdio:// --output kafka://localhost:9092 --stream clock
```

Here is how to stream from Kafka -> Stdio:

```sh
relay -- --input kafka://localhost:9092 --output stdio:// --stream clock
```

## `sea-streamer-kafka` SeaStreamer Kafka / Redpanda Backend

This is the Kafka / Redpanda backend implementation for SeaStreamer. Although the crate's name is `kafka`,
Redpanda integration is first-class as well. This crate depends on [`rdkafka`](https://docs.rs/rdkafka),
which in turn depends on [librdkafka-sys](https://docs.rs/librdkafka-sys), which itself is a wrapper of
[librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/index.html).

This crate provides a comprehensive type system that makes working with Kafka easier and safer.

## `sea-streamer-stdio` SeaStreamer Standard I/O Backend

This is the `stdio` backend implementation for SeaStreamer. It is designed to be connected together with unix pipes,
enabling great flexibility when developing stream processors or processing data locally.

You can connect processes together with pipes: `program_a | program_b`.

However you can also connect them asynchronously:

```sh
program_a > stream
tail -f stream | program_b
```

You can also use `cat` to replay a file, but it runs from start to end as fast as possible then stops,
which may or may not be the desired behavior.

You can write any valid UTF-8 string to stdin and each line will be considered a message. In addition, you can write some message meta in a simple format:

```log
[timestamp | stream key | sequence | shard_id] payload
```

Note: the square brackets are literal `[` `]`.

The following are all valid:

```log
a plain, raw message
[2022-01-01T00:00:00] { "payload": "anything" }
[2022-01-01T00:00:00.123 | my_topic] "a string payload"
[2022-01-01T00:00:00 | my-topic-2 | 123] ["array", "of", "values"]
[2022-01-01T00:00:00 | my-topic-2 | 123 | 4] { "payload": "anything" }
[my_topic] a string payload
[my_topic | 123] { "payload": "anything" }
[my_topic | 123 | 4] { "payload": "anything" }
```

The following are all invalid:

```log
[Jan 1, 2022] { "payload": "anything" }
[2022-01-01T00:00:00] 12345
```

If no stream key is given, it will be assigned the name `broadcast` and sent to all consumers.

You can create consumers that subscribe to only a subset of the topics.

Consumers in the same `ConsumerGroup` will be load balanced, meaning you can spawn multiple async tasks to process messages in parallel.
