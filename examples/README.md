# SeaStreamer Examples

This crate serves as a demo of SeaStreamer and also can be a starting point for you to develop your stream processors.

This crate works for both `tokio` and `async-std`, and streams to `kafka` and `stdio`.

+ `consumer`: A basic consumer
+ `producer`: A basic producer
+ `processor`: A basic stream processor
+ `buffered`: An advanced stream processor with internal buffering and batch processing

## Running the basic processor example

```sh
# Start a processor, waiting for input
cargo run --bin processor -- --input kafka://localhost:9092/hello1 --output kafka://localhost:9092/hello2 &
# Feed in some input
cargo run --bin producer -- --stream kafka://localhost:9092/hello1 &
# Replay the output
cargo run --bin consumer -- --stream kafka://localhost:9092/hello2
# Remember to stop the processor
kill %1
```

## Running the buffered processor example

The clock runs 10x faster than the processor, so we expect each batch consist of more or less 10 messages.

```sh
alias clock='cargo run --package sea-streamer-stdio --features=executables --bin clock'
clock -- --stream clock --interval 100ms | \
cargo run --bin buffered -- --input stdio:///clock --output stdio:///output
```

Output:

```log
[2023-02-27T10:43:58 | output | 0] [batch 0] { "tick": 0 } processed
[2023-02-27T10:43:59 | output | 1] [batch 1] { "tick": 1 } processed
[2023-02-27T10:43:59 | output | 2] [batch 1] { "tick": 2 } processed
[2023-02-27T10:43:59 | output | 3] [batch 1] { "tick": 3 } processed
[2023-02-27T10:43:59 | output | 4] [batch 1] { "tick": 4 } processed
[2023-02-27T10:43:59 | output | 5] [batch 1] { "tick": 5 } processed
[2023-02-27T10:43:59 | output | 6] [batch 1] { "tick": 6 } processed
[2023-02-27T10:43:59 | output | 7] [batch 1] { "tick": 7 } processed
[2023-02-27T10:43:59 | output | 8] [batch 1] { "tick": 8 } processed
...
```