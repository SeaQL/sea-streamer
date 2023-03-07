### `sea-streamer-file`: File Backend

This is very similar to `sea-streamer-stdio`, but the difference is `sea-streamer-stdio` works in real-time, while `sea-streamer-file` works in real-time and replay. That means, `sea-streamer-file` has the ability to seek through a `.ss` (sea-stream) file and seek/rewind to a particular timestamp/offset.

It will thus attempt to run through the messages as fast as possible (super-realtime).

In addition, `stdio` can only work with UTF-8 text data, while `file` is able to work with binary data.

We might be able to commit consumer states into a local SQLite database, enabling transactional behavior.

### Blockers

The status for Decimal serde format in Rust is incomplete. MessagePack has no native Decimal type, while BSON has but the `bson` crate does not. We will need to roll out a Decimal128 crate first...

Ref:
https://en.wikipedia.org/wiki/Decimal_floating_point
https://docs.rs/bson/latest/bson/struct.Decimal128.html
https://github.com/mongodb/specifications/pull/795
https://github.com/JohnAD/decimal128/blob/master/src/decimal128.nim
