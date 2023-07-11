#!/bin/bash
set -e

cd sea-streamer-types
cargo publish
cd ..

cd sea-streamer-runtime
cargo publish
cd ..

cd sea-streamer-stdio
cargo publish
cd ..

cd sea-streamer-file
cargo publish
cd ..

cd sea-streamer-kafka
cargo publish
cd ..

cd sea-streamer-redis
cargo publish
cd ..

cd sea-streamer-socket
cargo publish
cd ..

# publish `sea-streamer`
cargo publish

cd examples
cargo publish