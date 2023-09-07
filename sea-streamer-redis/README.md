### `sea-streamer-redis`: Redis Backend

This is the Redis backend implementation for SeaStreamer.
This crate provides a high-level async API on top of Redis that makes working with Redis Streams fool-proof:

+ Implements the familiar SeaStreamer abstract interface
+ A comprehensive type system that guides/restricts you with the API
+ High-level API, so you don't call `XADD`, `XREAD` or `XACK` anymore
+ Mutex-free implementation: concurrency achieved by message passing
+ Pipelined `XADD` and paged `XREAD`, with a throughput in the realm of 100k messages per second

While we'd like to provide a Kafka-like client experience, there are some fundamental differences between Redis and Kafka:

1. In Redis sequence numbers are not contiguous
    1. In Kafka sequence numbers are contiguous
2. In Redis messages are dispatched to consumers among group members in a first-ask-first-served manner, which leads to the next point
    1. In Kafka consumer <-> shard is 1 to 1 in a consumer group
3. In Redis `ACK` has to be done per message
    1. In Kafka only 1 Ack (read-up-to) is needed for a series of reads

What's already implemented:

+ RealTime mode with AutoStreamReset
+ Resumable mode with auto-ack and/or auto-commit
+ LoadBalanced mode with failover behaviour
+ Seek/rewind to point in time
+ Basic stream sharding: split a stream into multiple sub-streams

It's best to look through the [tests](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-redis/tests)
for an illustration of the different streaming behaviour.

How SeaStreamer offers better concurrency?

Consider the following simple stream processor:

```rust
loop {
    let input = XREAD.await;
    let output = process(input).await;
    XADD(output).await;
}
```

When it's reading or writing, it's not processing. So it's wasting time idle and reading messages with a higher delay, which in turn limits the throughput.
In addition, the ideal batch size for reads may not be the ideal batch size for writes.

With SeaStreamer, the read and write loops are separated from your process loop, so they can all happen in parallel (async in Rust is multi-threaded, so it is truely parallel)!

![](https://raw.githubusercontent.com/SeaQL/sea-streamer/main/sea-streamer-redis/docs/sea-streamer-concurrency.svg)

If you are reading from a consumer group, you also have to consider when to ACK and how many ACKs to batch in one request. SeaStreamer can commit in the background on a regular interval, or you can commit asynchronously without blocking your process loop.

In the future, we'd like to support Redis Cluster, because sharding without clustering is not very useful.
Right now it's pretty much a work-in-progress.
It's quite a difficult task, because clients have to take responsibility when working with a cluster.
In Redis, shards and nodes is a M-N mapping - shards can be moved among nodes *at any time*.
It makes testing much more difficult.
Let us know if you'd like to help!

There is also a [small utility](https://github.com/SeaQL/sea-streamer/tree/main/sea-streamer-redis/redis-streams-dump) to dump Redis Streams messages into a SeaStreamer file.

This crate is built on top of [`redis`](https://docs.rs/redis).
