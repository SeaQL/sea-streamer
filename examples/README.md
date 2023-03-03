# SeaStreamer Examples

This crate serves as a demo of SeaStreamer and also can be a starting point for you to develop your stream processors.

This crate works for both `tokio` and `async-std`, and streams to `kafka` and `stdio`.

+ `consumer`: A basic consumer
+ `producer`: A basic producer
+ `processor`: A basic stream processor
+ `resumable`: A resumable stream processor that continues from where it left off
+ `buffered`: An advanced stream processor with internal buffering and batch processing

## Running the basic processor example

With Kafka:

```bash
# Produce some input
cargo run --bin producer -- --stream kafka://localhost:9092/hello1 &
# Start the processor, producing some output
cargo run --bin processor -- --input kafka://localhost:9092/hello1 --output kafka://localhost:9092/hello2 &
# Replay the output
cargo run --bin consumer -- --stream kafka://localhost:9092/hello2
# Remember to stop the processes
kill %1 %2
```

With Stdio:

```bash
# Pipe the producer to the processor
cargo run --bin producer -- --stream stdio:///hello1 | \
cargo run --bin processor -- --input stdio:///hello1 --output stdio:///hello2
```

## Running the resumable processor example

The resumable processor can be killed anytime, and will continue from where it left off.
This is typically called "at least once" processing, meaning no messages should be skipped,
but it's possible for the same message to be processed twice.

```bash
# Produce lots of input
cargo run --bin producer -- --stream kafka://localhost:9092/hello1
# Run the processor, but kill it before it can process the entire stream
cargo run --bin resumable -- --input kafka://localhost:9092/hello1 --output stdio:///hello2 | head -n 10
cargo run --bin resumable -- --input kafka://localhost:9092/hello1 --output stdio:///hello2 | head -n 10
cargo run --bin resumable -- --input kafka://localhost:9092/hello1 --output stdio:///hello2 | head -n 10
```

Output:

```log
[2023-02-28T10:13:59 | hello2 | 0] "tick 0" processed
[2023-02-28T10:13:59 | hello2 | 1] "tick 1" processed
[2023-02-28T10:13:59 | hello2 | 2] "tick 2" processed
...
[2023-02-28T10:13:59 | hello2 | 9] "tick 9" processed
thread 'sea-streamer-stdio-stdout' panicked at 'failed printing to stdout: Broken pipe (os error 32)', library/std/src/io/stdio.rs:1009:9

[2023-02-28T10:14:08 | hello2 | 0] "tick 10" processed
...
[2023-02-28T10:14:08 | hello2 | 9] "tick 19" processed
thread 'sea-streamer-stdio-stdout' panicked at 'failed printing to stdout: Broken pipe (os error 32)', library/std/src/io/stdio.rs:1009:9

...
```

## Running the buffered processor example

The clock runs 10x faster than the processor, so we expect each batch consist of more or less 10 messages.

```bash
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

A more throughout technical discussion:

This pattern is useful when an input stream has a high frequency, but the processor has a high impedance.

For example, to insert records into a database, it's more efficient to insert in batches. The larger the batch, the more efficient.
But you can't naively fix the batch size at 10 or 100, because it might have buffered 9 messages and waiting for the 10th, and you can't handle a sudden burst of messages.

So, how to minimize the overall task execution time? You decouple the two busy loops and use a queue to act as a fluid coupling device - this is the best mechanical analogy I can make: now both loops can spin at their optimal frequency, maximizing the overall throughput of the processor.
