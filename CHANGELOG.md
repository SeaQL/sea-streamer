# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## 0.2.1 - 2023-05-07

+ Added a `MKSTREAM` option when creating Redis consumer groups (`RedisConsumerOptions::set_mkstream`) https://github.com/SeaQL/sea-streamer/pull/4
+ Added `SaslOptions` and `KafkaConnectOptions::set_sasl_options` for using Kafka with SASL authentication https://github.com/SeaQL/sea-streamer/pull/8

## 0.2.0 - 2023-03-25

Introducing `sea-streamer-redis`.

## 0.1.0 - 2023-03-03

Initial release.