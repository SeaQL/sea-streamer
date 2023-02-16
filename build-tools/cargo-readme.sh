set -e
# cargo install cargo-readme
cargo readme --no-badges --no-indent-headings --no-license --no-template --no-title > README.md
cd sea-streamer-kafka
cargo readme --no-badges --no-indent-headings --no-license --no-template --no-title > README.md
cd ../sea-streamer-runtime
cargo readme --no-badges --no-indent-headings --no-license --no-template --no-title > README.md
cd ../sea-streamer-socket
cargo readme --no-badges --no-indent-headings --no-license --no-template --no-title > README.md
cd ../sea-streamer-stdio
cargo readme --no-badges --no-indent-headings --no-license --no-template --no-title > README.md
cd ../sea-streamer-types
cargo readme --no-badges --no-indent-headings --no-license --no-template --no-title > README.md