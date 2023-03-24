### `sea-streamer-redis`: Redis Backend

> 🚧 To be released

This is the Redis backend implementation for SeaStreamer.
This crate provides a high-level async API on top of Redis that makes working with Redis Streams fool-proof:

+ Implements the familiar SeaStreamer abstract interface
+ A comprehensive type system that guides/restricts you with the API
+ High-level API, so you don't call `XADD`, `XREAD` or `XACK` anymore
+ Mutex-free implementation: concurrency achieved by message passing

While we'd like to provide a Kafka-like client experience, there are some fundamental differences between Redis and Kafka:

1. In Redis sequence numbers are not contiguous
2. In Redis messages are dispatched to consumers among group members in a first-ask-first-served manner, which leads to the next point
3. In Redis `ACK` has to be done per message

What's already implemented:

+ RealTime mode with AutoStreamReset
+ Resumable mode with 4 `ACK` mechanisms
+ LoadBalanced mode with failover
+ Seek/rewind to point in time
+ Basic stream sharding: split a stream into multiple sub-streams

Why SeaStreamer offers better concurrency?

Consider the following simple program:

```rust
loop {
    let messages = XREAD.await
    process(messages).await
}
```

While it's reading, its not processing. So it's wasting idle time and reading messages with a higher delay.

In SeaStreamer, the read loop is separate from your process loop, so they can happen in parallel.

If you are reading from a consumer group, you also have to consider when to ACK and how many ACKs to batch in one request.

SeaStreamer can commit in the background on a regular interval, or you can commit asynchronously without blocking your process loop.

In the future, we'd like to support Redis Cluster, because sharding without clustering is not very useful.
Right now it's pretty much a work-in-progress.
It's quite a difficult task, because clients have to take responsibility when working with a cluster.
In Redis, shards and nodes is a M-N mapping - shards can be moved among nodes *at any time*.
It makes testing much more difficult.
Let us know if you'd like to help!
