# 🌊 SeaStreamer File Backend

This is very similar to `sea-streamer-stdio`, but the difference is `sea-streamer-stdio` works in real-time, while `sea-streamer-file` works in replay.

Thus, `sea-streamer-file` has the ability to seek through a `.ss` (sea-stream) file and seek/rewind to a particular timestamp/offset.

It will thus attempt to run through the messages as fast as possible (super-realtime).

In addition, we might be able to commit consumer states into a local SQLite database, enabling transactional behavior.