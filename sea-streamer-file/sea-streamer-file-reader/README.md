### `sea-streamer-file-reader`: File Reader for node.js

This library implements a decoder for the SeaStreamer file format.

It does not provide the high-level Consumer interface. But the reason it is so complex in Rust is due to multi-threaded concurrency. Since node.js is basically single threaded, this made the implementation much simpler.