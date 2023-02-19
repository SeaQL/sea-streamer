<div align="center">

  <img src="./docs/SeaStreamer Banner.png"/>

  <h1>SeaStreamer</h1>

  <p>
    <strong>🌊 The stream processing toolkit for Rust</strong>
  </p>

  [![crate](https://img.shields.io/crates/v/sea-streamer.svg)](https://crates.io/crates/sea-streamer)
  [![docs](https://docs.rs/sea-streamer/badge.svg)](https://docs.rs/sea-streamer)
  [![build status](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml/badge.svg)](https://github.com/SeaQL/sea-streamer/actions/workflows/rust.yml)

</div>

SeaStreamer is a stream processing toolkit to help you build stream processors in Rust.

## Features

1. Async

SeaStreamer provides an async API, and it supports both `tokio` and `async-std`. In tandem with other async Rust libraries,
you can build highly concurrent stream processors.

2. Generic

We provide integration for Kafka / Redpanda behind a generic trait interface, so your program can be backend-agnostic.
Support for Redis Stream is being planned.

3. Testable

SeaStreamer also provides a set of tools to work with streams via unix pipes, so it is testable without setting up a cluster,
and extremely handy when working locally.

4. Micro-service Oriented

Let's build real-time (multi-threaded, no GC), self-contained (aka easy to deploy), low-resource-usage, long-running (no memory leak) stream processors in Rust!

## Quick Start

Add the following to your `Cargo.toml`

```toml
sea-streamer = { version = "0", features = ["socket", "kafka", "runtime-tokio"] }
```

Here is a basic stream consumer, [full example](https://github.com/SeaQL/sea-streamer/tree/main/examples/src/bin/consumer.rs):

```rust
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::from_args();

    let streamer = SeaStreamer::connect(
        std::env::var("STREAMER_URL")
            .unwrap_or_else(|_| "kafka://localhost:9092".to_owned())
            .parse()?,
        Default::default(),
    )
    .await?;

    let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_kafka_consumer_options(|options| {
        options.set_auto_offset_reset(AutoOffsetReset::Earliest);
    });
    let consumer: SeaConsumer = streamer.create_consumer(&[stream], options).await?;

    loop {
        let mess: SeaMessage = consumer.next().await?;
        println!("[{}] {}", mess.timestamp(), mess.message().as_str()?);
    }
}
```

Here is a basic stream producer, [full example](https://github.com/SeaQL/sea-streamer/tree/main/examples/src/bin/producer.rs):

```rust
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::from_args();

    let streamer = SeaStreamer::connect(
        std::env::var("STREAMER_URL")
            .unwrap_or_else(|_| "kafka://localhost:9092".to_owned())
            .parse()?,
        Default::default(),
    )
    .await?;

    let producer: SeaProducer = streamer.create_producer(stream, Default::default()).await?;

    for tick in 0..10 {
        let message = format!(r#""tick {tick}""#);
        println!("{message}");
        producer.send(message)?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    producer.flush(Duration::from_secs(10)).await?;

    Ok(())
}
```

## Architecture

`sea-streamer` is the facade crate re-exporting implementation from a number of sub-crates:

+ `sea-streamer-types`
+ `sea-streamer-socket`
    + `sea-streamer-kafka`
    + `sea-streamer-stdio`

[`sea-streamer` API Docs](https://docs.rs/sea-streamer)

## SeaStreamer Types

This crate defines all the traits and types for the SeaStreamer API, but does not provide any implementation.

[`sea-streamer-types` API Docs](https://docs.rs/sea-streamer-types)

## SeaStreamer backend-agnostic Socket API

Akin to how SeaORM allows you to build applications for different databases, SeaStreamer allows you to build
stream processors for different streaming servers.

[`sea-streamer-socket` API Docs](https://docs.rs/sea-streamer-socket)

While the `sea-streamer-types` crate provides a nice trait-based abstraction, this crates provides a concrete-type API,
so that your program can stream from/to any SeaStreamer backend selected by the user *on runtime*.

This allows you to do neat things, like generating data locally and then stream them to Kafka. Or in the other
way, sink data from Kafka to work on them locally. All _without recompiling_ the stream processor.

If you only ever work with Kafka, feel free to depend on `sea-streamer-kafka` directly.

A small number of cli programs are provided for demonstration. Let's set them up first:

```sh
# The `clock` program generate messages in the form of `{ "tick": N }`
alias clock='cargo run --package sea-streamer-stdio  --features=executables --bin clock'
# The `relay` program redirect messages from `input` to `output`
alias relay='cargo run --package sea-streamer-socket --features=executables --bin relay'
```

Here is how to stream from Stdio ➡️ Kafka. We generate messages using `clock` and then pipe it to `relay`,
which then streams to Kafka:

```sh
clock -- --stream clock --interval 1s | \
relay -- --stream clock --input stdio:// --output kafka://localhost:9092
```

Here is how to *replay* the stream from Kafka ➡️ Stdio:

```sh
relay -- --stream clock --input kafka://localhost:9092 --output stdio:// --offset start
```

## SeaStreamer Kafka / Redpanda Backend

This is the Kafka / Redpanda backend implementation for SeaStreamer.
This crate provides a comprehensive type system that makes working with Kafka easier and safer.

[`sea-streamer-kafka` API Docs](https://docs.rs/sea-streamer-kafka)

`KafkaConsumerOptions` has typed parameters.

`KafkaConsumer` allows you to `seek` to point in time, `rewind` to particular offset, and `commit` message read.

`KafkaProducer` allows you to `await` a send `Receipt` or discard it if you are uninterested.

`KafkaStreamer` allows you to flush all producers on `disconnect`.

This crate depends on [`rdkafka`](https://docs.rs/rdkafka),
which in turn depends on [librdkafka-sys](https://docs.rs/librdkafka-sys), which itself is a wrapper of
[librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/index.html).

## SeaStreamer Standard I/O Backend

This is the `stdio` backend implementation for SeaStreamer. It is designed to be connected together with unix pipes,
enabling great flexibility when developing stream processors or processing data locally.

[`sea-streamer-stdio` API Docs](https://docs.rs/sea-streamer-stdio)

You can connect processes together with pipes: `program_a | program_b`.

You can also connect them asynchronously:

```sh
touch stream # set up an empty file
tail -f stream | program_b # program b can be spawned anytime
program_a >> stream # append to the file
```

You can also use `cat` to replay a file, but it runs from start to end as fast as possible then stops,
which may or may not be the desired behavior.

You can write any valid UTF-8 string to stdin and each line will be considered a message. In addition, you can write some message meta in a simple format:

```log
[timestamp | stream_key | sequence | shard_id] payload
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

Consumers in the same `ConsumerGroup` will be load balanced (in a round-robin fashion), meaning you can spawn multiple async tasks to process messages in parallel.

## License

Licensed under either of

-   Apache License, Version 2.0
    ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
-   MIT license
    ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

## Sponsor

Our [GitHub Sponsor](https://github.com/sponsors/SeaQL) profile is up! SeaQL.org is an independent open-source organization run by passionate developers. If you enjoy using SeaORM, please star and share our repositories. If you feel generous, a small donation will be greatly appreciated, and goes a long way towards sustaining the project.

We invite you to participate, contribute and together help build Rust's future.
