## SeaStreamer Kafka / Redpanda Backend

This is the Kafka / Redpanda backend implementation for SeaStreamer. Although the crate's name is `kafka`,
Redpanda integration is first-class as well. This crate depends on [`rdkafka`](https://docs.rs/rdkafka),
which in turn depends on [librdkafka-sys](https://docs.rs/librdkafka-sys), which itself is a wrapper of
[librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/index.html).

This crate provides a comprehensive type system that makes working with Kafka easier and safer.

[`sea-streamer-kafka` API Docs](https://docs.rs/sea-streamer-kafka)
