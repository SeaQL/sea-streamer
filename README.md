# SeaStreamer

The universal stream processing toolkit

# Background

https://www.confluent.io/blog/turning-the-database-inside-out-with-apache-samza/ is an influential article in the data engineering space, and lead to the founding of [Kafka](https://kafka.apache.org/). Since then, competing implementations like [Redpanda](https://redpanda.com/) and [Redis Streams](https://redis.io/docs/manual/data-types/streams/) emerged, spurring a real-time data processing ecosystem, with projects like [ksqlDB](https://ksqldb.io/) and [Materialize](https://materialize.com/) built on top / around.

# Statement of Intent

Similar to what SeaORM promises, we want to make Rust the best language for stream processing, and construct a data engineering platform (free of JVM drawbacks) featuring Rust's low latency, low memory footprint, suitable for long running (no GC pause and memory leaks).

Similar to SeaORM, we will provide a Stream library which is high-level, abstract and backend agnostic.

Similar to SeaQuery, we will provide a generic driver library implementing the `Streamer` trait to support different backends.

Similar to SeaSchema, we will also provide a programmable API for administration of brokers and clusters.

(But we may keep everything under the same repo for now)

Finally, we want to provide an array of command line tools for manual data maintenance, usable in the unix shell!

Let me illustrate this extravagant concept:

```sh
# we setup a `flux` which maps a unix file to a stream producer
$ sea streamer flux ./producer --broker localhost:9092 --topic news &
# we want to stream the content of the Newline Delimited JSON file
$ cat news.ndjson > producer
# kill the process and then the ./producer file will be unlinked
$ kill %1
```

Likewise, we can tap in a stream:

```sh
$ sea streamer tap ./consumer --broker localhost:9092 --topic news &
$ cat consumer > news.ndjson
$ kill %1
```

Sweet, isn't it?

# Concepts

### Stream

A stream (it's actually a topic in Kafka) consist of a series of messages with a timestamp, sequence number (known as offset in Kafka) and shard id (known as partition number in Kafka). A message is uniquely identified by the (shard id, sequence number) pair.

The stream can be sought to a particular timestamp or sequence number.

Stream data has a retention period (how long before data will be deleted).

### Consumer

A stream consumer subscribes to one or more streams and receive messages from one or more node in the cluster.

According to the use case, there can be several consumption preferences:

1. latest: we only care about latest messages and would be okay to miss old data
2. repeat: we should process all messages, but wouldn't mind processing the same message more than once
3. exactly once: each message must be processed and be processed exactly once

### Producer

A stream producer send messages to a broker, and the broker would forward to a node in the cluster. There can be logic in how to shard a stream, but usually it's pseudo-random.

According to the use case, there can be several durability requirements:

1. fire and forget (at most once): basically no guarantee that a message will be persisted
2. at least once: we would try to deliver the message only once, but might end up more than once upon network failure (basically we want to retry until we receive an ack)
3. exactly once: basically the broker has to have a buffer to be able to remove duplicate messages, which means we cannot guarantee uniqueness across the entire stream, only a specific time window