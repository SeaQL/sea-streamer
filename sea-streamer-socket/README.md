### `sea-streamer-socket`: Backend-agnostic Socket API

Akin to how SeaORM allows you to build applications for different databases, SeaStreamer allows you to build
stream processors for different streaming servers.

While the `sea-streamer-types` crate provides a nice trait-based abstraction, this crates provides a concrete-type API,
so that your program can stream from/to any SeaStreamer backend selected by the user *on runtime*.

This allows you to do neat things, like generating data locally and then stream them to Redis / Kafka. Or in the other
way, sink data from server to work on them locally. All _without recompiling_ the stream processor.

If you only ever work with one backend, feel free to depend on `sea-streamer-redis` / `sea-streamer-kafka` directly.

A small number of cli programs are provided for demonstration. Let's set them up first:

```shell
# The `clock` program generate messages in the form of `{ "tick": N }`
alias clock='cargo run --package sea-streamer-stdio  --features=executables --bin clock'
# The `relay` program redirect messages from `input` to `output`
alias relay='cargo run --package sea-streamer-socket --features=executables,backend-kafka,backend-redis --bin relay'
```

Here is how to stream from Stdio ➡️ Redis / Kafka. We generate messages using `clock` and then pipe it to `relay`,
which then streams to Redis / Kafka:

```shell
# Stdio -> Redis
clock -- --stream clock --interval 1s | \
relay -- --input stdio:///clock --output redis://localhost:6379/clock
# Stdio -> Kafka
clock -- --stream clock --interval 1s | \
relay -- --input stdio:///clock --output kafka://localhost:9092/clock
```

Here is how to stream between Redis ↔️ Kafka:

```shell
# Redis -> Kafka
relay -- --input redis://localhost:6379/clock --output kafka://localhost:9092/clock
# Kafka -> Redis
relay -- --input kafka://localhost:9092/clock --output redis://localhost:6379/clock
```

Here is how to *replay* the stream from Kafka / Redis:

```shell
relay -- --input redis://localhost:6379/clock --output stdio:///clock --offset start
relay -- --input kafka://localhost:9092/clock --output stdio:///clock --offset start
```
