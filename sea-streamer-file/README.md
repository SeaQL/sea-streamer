### `sea-streamer-file`: File Backend

This is very similar to `sea-streamer-stdio`, but the difference is `sea-streamer-stdio` works in real-time, while `sea-streamer-file` works in real-time and replay. That means, `sea-streamer-file` has the ability to seek through a `.ss` (sea-stream) file and seek/rewind to a particular timestamp/offset.

In addition, `stdio` can only work with UTF-8 text data, while `file` is able to work with binary data.

We might be able to commit consumer states into a local SQLite database, enabling transactional behavior.
