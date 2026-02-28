# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## Pending

### Upgrades

* Upgrade Rust edition to 2024, MSRV to 1.85
* Upgrade `redis` to `1.0` (breaking: `Value` enum renames, `ErrorKind` restructuring, `ConnectionInfo` builder pattern)
* Upgrade `thiserror` to `2`
* Upgrade `env_logger` to `0.11`
* Upgrade `fastrand` to `2`
* Upgrade `rdkafka` to `0.37`
* Upgrade `async-tungstenite` to `0.33`
* Upgrade `sea-orm` to `2.0.0-rc.35` (in examples)

### Enhancements

* Replace `async-std` with `smol` as the second supported runtime (feature `runtime-smol`)
* Replace `lazy_static` with `std::sync::LazyLock`

### House keeping

* Fix clippy warnings for edition 2024 (lifetime syntax, import ordering)
* Update CI: Kafka 4.2/3.9 with KRaft mode, smol runtime matrix

## 0.5.2 - 2024-11-30

### `sea-streamer-types`

* Added `From<Url>` and `FromIterator<Url>` for `StreamerUri` https://github.com/SeaQL/sea-streamer/pull/28
* Impl `Default` for `Payload`
* Impl serde `Serialize` & `Deserialize` for `StreamKey`

### `sea-streamer-socket`

* Compile socket without stdio backend https://github.com/SeaQL/sea-streamer/pull/35

### `sea-streamer-redis`

* Support nanosecond timestamp in Redis (under feature flag `nanosecond-timestamp`)
* Support custom message field
* Added `RedisProducer::send_with_ts` to specify custom timestamp
* Added `RedisProducer::flush_immut` 
* Added `RedisProducer::trim` to perform `XTRIM MAXLEN`
* Fixed `capacity overflow` error in some cases

### `sea-streamer-file`

* Added a special `SEA_STREAMER_WILDCARD` stream key to subscribe to all streams in a file

### `sea-streamer-fuse`

* Added a `StreamJoin` component for joining multiple streams by timestamp

## 0.5.0 - 2024-04-24

### Upgrades

* Migrate to `clap` https://github.com/SeaQL/sea-streamer/pull/19
* Upgrade `flume` to `0.11`
* Upgrade `rdkafka` to `0.36` https://github.com/SeaQL/sea-streamer/pull/22
* Upgrade `redis` to `0.25` https://github.com/SeaQL/sea-streamer/pull/21

### Enhancements

* Use `impl Future` instead of `async-trait` https://github.com/SeaQL/sea-streamer/pull/20

### House keeping

* Fix clippy warnings https://github.com/SeaQL/sea-streamer/pull/23

## 0.3.x

### `sea-streamer-file` 0.3.9 - 2023-12-04

* Rename utility to `ss-decode`

### `sea-streamer-kafka` 0.3.2 - 2023-11-19

* Added `KafkaProducer::send_record`, `KafkaProducer::send_message` https://github.com/SeaQL/sea-streamer/pull/17

### `sea-streamer-file` 0.3.8 - 2023-11-17

* Added `FileSource::drain`

### `sea-streamer-file` 0.3.7 - 2023-10-18

* Added `FileProducer::path()`, `FileConsumer::file_id()`, `FileProducer::file_id()`

### `sea-streamer-file` 0.3.6 - 2023-10-04

* Added `create_only` to `FileConnectOptions`

### `sea-streamer-file` 0.3.5 - 2023-09-20

* Fixed a potential race condition

### `sea-streamer-file` 0.3.4 - 2023-09-15

* impl std::io::Write for FileSink
* More precise `FileEvent::Remove`
* End streamer properly after EOS

### `sea-streamer-file` 0.3.3 - 2023-09-06

* Used a faster CRC implementation
* Added option `prefetch_message` to `FileConnectOptions`

## 0.3.2 - 2023-09-05

+ [`sea-streamer-file`] Improvements over read and write throughput
+ [`sea-streamer-file-reader`] A node.js library for decoding sea-streamer files
+ [`sea-streamer-redis`] Improvements over write throughput

### `sea-streamer-file` 0.3.1 - 2023-08-21

* Enhance decoder to display binary payload for JSON
* Fix shared producer https://github.com/SeaQL/sea-streamer/pull/11
    Previously, when a FileProducer is cloned, dropping any clone would implicitly end the producer.

## 0.3.0 - 2023-07-11

* Introducing `sea-streamer-file`: the File Backend
* Added `File`, `OpenOptions`, `AsyncReadExt` etc to `sea-streamer-runtime`
* Added `AsyncMutex` to `sea-streamer-runtime`
* Added `OwnedMessage` to `sea-streamer-types`
* Added `TIMESTAMP_FORMAT` and `SEA_STREAMER_INTERNAL` to `sea-streamer-types`
* Implemented `serde::Serialize` for `MessageHeader`

### Breaking changes

* Removed const `SEA_STREAMER_INTERNAL` from `sea-streamer-redis`
* `StreamUrl` now requires an ending slash to avoid ambiguity, `StreamerUri` remains unchanged
```rust
assert!("redis://localhost/a,b".parse::<StreamUrl>().is_ok());
assert!("redis://localhost/".parse::<StreamUrl>().is_ok());
assert!("redis://localhost".parse::<StreamUrl>().is_err()); // previously this was OK
assert!("redis://localhost".parse::<StreamerUri>().is_ok());
```

## 0.2.1 - 2023-05-07

* Added a `MKSTREAM` option when creating Redis consumer groups (`RedisConsumerOptions::set_mkstream`) https://github.com/SeaQL/sea-streamer/pull/4
* Added `SaslOptions` and `KafkaConnectOptions::set_sasl_options` for using Kafka with SASL authentication https://github.com/SeaQL/sea-streamer/pull/8

## 0.2.0 - 2023-03-25

Introducing `sea-streamer-redis`.

## 0.1.0 - 2023-03-03

Initial release.
