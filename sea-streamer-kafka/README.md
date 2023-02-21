## SeaStreamer Kafka / Redpanda Backend

This is the Kafka / Redpanda backend implementation for SeaStreamer.
This crate provides a comprehensive type system that makes working with Kafka easier and safer.

[`sea-streamer-kafka` API Docs](https://docs.rs/sea-streamer-kafka)

`KafkaConsumerOptions` has typed parameters.

`KafkaConsumer` allows you to `seek` to point in time, `rewind` to particular offset, and `commit` message read.

`KafkaProducer` allows you to `await` a send `Receipt` or discard it if you are uninterested. You can also flush the Producer.

`KafkaStreamer` allows you to flush all producers on `disconnect`.

See [tests](https://github.com/SeaQL/sea-streamer/blob/main/sea-streamer-kafka/tests/consumer.rs) for an illustration of the stream semantics.

This crate depends on [`rdkafka`](https://docs.rs/rdkafka),
which in turn depends on [librdkafka-sys](https://docs.rs/librdkafka-sys), which itself is a wrapper of
[librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/index.html).
