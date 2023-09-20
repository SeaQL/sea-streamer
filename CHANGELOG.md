# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

### `sea-streamer-file` 0.3.5 - 2023-09-20

+ Fixed a potential race condition

### `sea-streamer-file` 0.3.4 - 2023-09-15

+ impl std::io::Write for FileSink
+ More precise `FileEvent::Remove`
+ End streamer properly after EOS

### `sea-streamer-file` 0.3.3 - 2023-09-06

+ Used a faster CRC implementation
+ Added option `prefetch_message` to `FileConnectOptions`

## 0.3.2 - 2023-09-05

+ [`sea-streamer-file`] Improvements over read and write throughput
+ [`sea-streamer-file-reader`] A node.js library for decoding sea-streamer files
+ [`sea-streamer-redis`] Improvements over write throughput

### `sea-streamer-file` 0.3.1 - 2023-08-21

+ Enhance decoder to display binary payload for JSON
+ Fix shared producer https://github.com/SeaQL/sea-streamer/pull/11
    Previously, when a FileProducer is cloned, dropping any clone would implicitly end the producer.

## 0.3.0 - 2023-07-11

+ Introducing `sea-streamer-file`: the File Backend
+ Added `File`, `OpenOptions`, `AsyncReadExt` etc to `sea-streamer-runtime`
+ Added `AsyncMutex` to `sea-streamer-runtime`
+ Added `OwnedMessage` to `sea-streamer-types`
+ Added `TIMESTAMP_FORMAT` and `SEA_STREAMER_INTERNAL` to `sea-streamer-types`
+ Implemented `serde::Serialize` for `MessageHeader`

### Breaking changes

+ Removed const `SEA_STREAMER_INTERNAL` from `sea-streamer-redis`
+ `StreamUrl` now requires an ending slash to avoid ambiguity, `StreamerUri` remains unchanged
```rust
assert!("redis://localhost/a,b".parse::<StreamUrl>().is_ok());
assert!("redis://localhost/".parse::<StreamUrl>().is_ok());
assert!("redis://localhost".parse::<StreamUrl>().is_err()); // previously this was OK
assert!("redis://localhost".parse::<StreamerUri>().is_ok());
```

## 0.2.1 - 2023-05-07

+ Added a `MKSTREAM` option when creating Redis consumer groups (`RedisConsumerOptions::set_mkstream`) https://github.com/SeaQL/sea-streamer/pull/4
+ Added `SaslOptions` and `KafkaConnectOptions::set_sasl_options` for using Kafka with SASL authentication https://github.com/SeaQL/sea-streamer/pull/8

## 0.2.0 - 2023-03-25

Introducing `sea-streamer-redis`.

## 0.1.0 - 2023-03-03

Initial release.