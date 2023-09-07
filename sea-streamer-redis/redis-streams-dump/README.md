# Redis Streams Dump

A small utility to dump Redis Streams content into a SeaStreamer file.

```sh
cargo install redis-streams-dump
```

```sh
redis-streams-dump --stream redis://localhost/clock --output ./clock.ss --since 2023-09-05T13:30:00.7 --until 2023-09-05T13:30:00.8
```