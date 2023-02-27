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