🌊 SeaStreamer backend-agnostic Socket API

Akin to how SeaORM allows you to build applications for different databases, SeaStreamer allows you to build
stream processors for any streaming server.

While the `sea-streamer-types` provides a nice trait-based abstraction, this crates provides a concrete API,
so that your program can stream to any SeaStreamer backend selected by the user *on runtime*.

This allows you to do neat things, like generating data locally and then stream them to Kafka. Or in the other
way, sink data from Kafka to work on them locally. All _without recompiling_ the stream processor.

If you only ever work with Kafka, feel free to depend on `sea-streamer-kafka` directly.

```sh
alias relay='cargo run --package sea-streamer-socket --bin relay --features=executables'
alias clock='cargo run --package sea-streamer-stdio  --bin clock --features=executables'
```

Here is how to stream from Stdio -> Kafka. We generate messages using `clock` and then pipe it to `relay`,
which then streams to Kafka.

```sh
clock -- --interval 1s --stream clock | \
relay -- --input stdio:// --output kafka://localhost:9092 --stream clock
```

Here is how to stream from Kafka -> Stdio

```sh
relay -- --input kafka://localhost:9092 --output stdio:// --stream clock
```
