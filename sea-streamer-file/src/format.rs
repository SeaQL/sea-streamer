//! The SeaStreamer file format is designed to be seekable.
//! It has internal checksum to ensure integrity.
//! It is a binary file format, but is readable with a plain text editor (if the payload is UTF-8).
//!
//! There is a header. Every N bytes after the header there will be a beacon summarizing all streams so far.
//! A message can be spliced by a beacon.
//!
//! ```ignore
//! +-----------------~-----------------+
//! |               Header              |
//! +-----------------~-----------------+
//! |               Message             |
//! +-----------------~-----------------+
//! |               Message ...         |
//! +-----------------~-----------------+
//! |               Beacon              |
//! +-----------------~-----------------+
//! |               Message ...         |
//! +-----------------~-----------------+
//!
//! Header is:
//! +--------+--------+---------+---~---+
//! |  0x53  |  0x73  | version | meta  |
//! +--------+--------+---------+---~---+
//!
//! Header meta v1 is always 128 - 3 bytes long. Unused bytes must be zero-padded.
//!
//! Message is:
//! +---~----+--------+--------+----~----+----------+------+
//! | header | size of payload | payload | checksum | 0x0D |
//! +---~----+--------+--------+----~----+----------+------+
//!
//! Message spliced:
//! +----~----+----~---+--------~-------+
//! | message | beacon | message cont'd |
//! +----~----+----~---+--------~-------+
//!
//! Beacon is:
//! +------------+------------+--------------+----~----+-----+
//! | remaining message bytes | num of items |  item   | ... |
//! +------------+------------+--------------+----~----+-----+
//!
//! Message header is same as beacon item:
//! +-------------------+--------~---------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | len of stream key | stream key chars |   shard id    |     seq no    |   timestamp   |
//! +-------------------+--------~---------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//!
//! Except that beacon item has an extra tail:
//! +------------------+
//! | running checksum |
//! +------------------+
//! ```
//!
//! All numbers are encoded in big endian.
