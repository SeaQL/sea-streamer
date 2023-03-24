#!/bin/bash
set -e

cd sea-streamer-types
cargo publish
cd ..
sleep 60

cd sea-streamer-runtime
cargo publish
cd ..
sleep 60

cd sea-streamer-stdio
cargo publish
cd ..
sleep 60

cd sea-streamer-kafka
cargo publish
cd ..
sleep 60

cd sea-streamer-redis
cargo publish
cd ..
sleep 60

cd sea-streamer-socket
cargo publish
cd ..
sleep 60

# publish `sea-streamer`
cargo publish
sleep 60

cd examples
cargo publish