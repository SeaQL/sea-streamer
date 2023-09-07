# Redis Streams Dump

A small utility to dump Redis Streams messages into a SeaStreamer file.

```sh
cargo install redis-streams-dump
```

```sh
redis-streams-dump --stream redis://localhost/clock --output ./clock.ss --since 2023-09-05T13:30:00.7 --until 2023-09-05T13:30:00.8
# OR in the workspace
cargo run --package redis-streams-dump -- ..
```

```sh
redis-streams-dump

USAGE:
    redis-streams-dump [OPTIONS] --output <output> --stream <stream>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --output <output>    Output file. Overwrites if exist
        --since <since>      Timestamp start of range
        --stream <stream>    Streamer URI with stream key, i.e. try `redis://localhost/hello`
        --until <until>      Timestamp end of range
```