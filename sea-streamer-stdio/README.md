# 🌊 SeaStreamer standard I/O Backend

This is stdio backend implementing SeaStreamer traits. It is designed to be connected together with unix pipes, enabling great flexibility developing stream processors or processing data locally.

You can write anything to stdin and each line will be considered a message.

In addition, you can write some message meta:

```
[timestamp | stream key | sequence | shard_id] payload
```

The following are all valid:

```
[2022-01-01T00:00:00] { "payload": "anything" }
[2022-01-01T00:00:00 | my_topic] "A string payload"
[2022-01-01T00:00:00 | my-topic-2 | 123] ["array", "of", "values"]
[2022-01-01T00:00:00 | my-topic-2 | 123 | 4] { "payload": "anything" }
[my_topic] "A string payload"
[my_topic | 123] { "payload": "anything" }
[my_topic | 123 | 4] { "payload": "anything" }
```

As such, you can create consumers and filter messages by stream key.

If no stream key is given, it will be assigned the name `broadcast` and sent to all consumers.

Consumers in the same `ConsumerGroup` will be load balanced, meaning you can spawn multiple async tasks to process messages in parallel.