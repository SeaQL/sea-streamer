# SeaStreamer Examples

This crate serves as a demo of SeaStreamer and also can be a starting point for you to develop your stream processors, while demonstrating various stream processing techniques.

This crate works for both `tokio` and `async-std`, and streams to `kafka` and `stdio`.

+ `consumer`: A basic consumer
+ `producer`: A basic producer
+ `processor`: A basic stream processor
+ `resumable`: A resumable stream processor that continues from where it left off
+ `buffered`: An advanced stream processor with internal buffering and batch processing
+ `blocking`: An advanced stream processor for handling blocking / CPU-bound tasks

+ `price-feed`: A websocket to Redis / Kafka stream producer
+ `sea-orm-sink`: A Redis / Kafka to SQLite data sink

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

With Redis:

```bash
# Produce some input
cargo run --bin producer -- --stream redis://localhost:6379/hello1 &
# Start the processor, producing some output
cargo run --bin processor -- --input redis://localhost:6379/hello1 --output redis://localhost:6379/hello2 &
# Replay the output
cargo run --bin consumer -- --stream redis://localhost:6379/hello2
# Remember to stop the processes
kill %1 %2
```

With File:

```bash
# Create the file
file=/tmp/sea-streamer-$(date +%s)
touch $file && echo "File created at $file"
# Produce some input
cargo run --bin producer -- --stream file://$file/hello &
# Replay the input
cargo run --bin consumer -- --stream file://$file/hello
# Start the processor, producing some output
cargo run --bin processor -- --input file://$file/hello --output stdio:///hello
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
STREAMER_URI="kafka://localhost:9092"
STREAMER_URI="redis://localhost:6379"
# Produce lots of input
cargo run --bin producer -- --stream $STREAMER_URI/hello1
# Run the processor, but kill it before it can process the entire stream
cargo run --bin resumable -- --input $STREAMER_URI/hello1 --output stdio:///hello2 | head -n 10
cargo run --bin resumable -- --input $STREAMER_URI/hello1 --output stdio:///hello2 | head -n 10
cargo run --bin resumable -- --input $STREAMER_URI/hello1 --output stdio:///hello2 | head -n 10
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

This pattern is useful when an input stream has a high frequency, but the processor has a high impedance.

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

For example, to insert records into a database, it's more efficient to insert in batches.
But you can't naively fix the batch size at 10 or 100, because it might have buffered 9 messages and waiting for the 10th, and you can't handle a sudden burst of messages.

So, how to minimize the overall task execution time? You decouple the two busy loops and use a queue to act as a fluid coupling device - this is the best mechanical analogy I can make: now both loops can spin at their optimal frequency, maximizing the overall throughput of the processor.

## Running the blocking processor example

The clock runs 3x faster than the processor, but we have 4 threads, so we expect it to be able to catch up in real-time. Tasks are randomly assigned to threads, aka. a "fan out" pattern.

This pattern is useful when you have to perform blocking IO or CPU-heavy computation.

```bash
alias clock='cargo run --package sea-streamer-stdio --features=executables --bin clock'
clock -- --stream clock --interval 333ms | \
cargo run --bin blocking -- --input stdio:///clock --output stdio:///output
```

Output:

```log
[2023-03-07T06:00:52 | output | 0] [thread 0] { "tick": 0 } processed
[2023-03-07T06:00:53 | output | 1] [thread 1] { "tick": 1 } processed
[2023-03-07T06:00:53 | output | 2] [thread 2] { "tick": 2 } processed
[2023-03-07T06:00:53 | output | 3] [thread 3] { "tick": 3 } processed
[2023-03-07T06:00:54 | output | 4] [thread 0] { "tick": 4 } processed
[2023-03-07T06:00:54 | output | 5] [thread 1] { "tick": 5 } processed
[2023-03-07T06:00:54 | output | 6] [thread 2] { "tick": 6 } processed
```