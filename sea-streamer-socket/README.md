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
relay --  --stream clock --input stdio:// --output kafka://localhost:9092
```

Here is how to replay the stream from Kafka -> Stdio:

```sh
relay --  --stream clock --input kafka://localhost:9092 --output stdio:// --offset start
```
