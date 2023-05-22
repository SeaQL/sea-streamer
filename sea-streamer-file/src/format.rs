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
//! +---~----+---+----+---+----+----~----+----------+------+
//! | header | size of payload | payload | checksum | 0x0D |
//! +---~----+---+----+---+----+----~----+----------+------+
//!
//! Message spliced:
//! +----~----+----~---+--------~-------+
//! | message | beacon | message cont'd |
//! +----~----+----~---+--------~-------+
//!
//! Beacon is:
//! +-----+------+-----+------+---+---+---+--+----~----+-----+
//! | remaining message bytes | num of items |  item   | ... |
//! +-----+------+-----+------+---+---+---+--+----~----+-----+
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

use crate::{Bytes, FileErr, FileSink, FileSource};
use sea_streamer_types::{ShardId, StreamKey, StreamKeyErr, Timestamp};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderV1 {
    pub file_name: String,
    pub created_at: Timestamp,
    pub beacon_interval: u16,
}

pub type Header = HeaderV1;

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct MessageHeader(pub sea_streamer_types::MessageHeader);

pub use short_string::*;

/// Timestamp in seconds
#[repr(transparent)]
pub struct UnixTimestamp(pub Timestamp);

#[repr(transparent)]
pub struct U64(pub u64);

#[repr(transparent)]
pub struct U32(pub u32);

#[repr(transparent)]
pub struct U16(pub u16);

#[derive(Error, Debug)]
pub enum HeaderErr {
    #[error("Byte mark mismatch")]
    ByteMark,
    #[error("Version mismatch")]
    Version,
}

#[derive(Error, Debug)]
pub enum FormatErr {
    #[error("ShortStringErr: {0}")]
    ShortStringErr(#[from] ShortStringErr),
    #[error("UnixTimestampErr: {0}")]
    UnixTimestampErr(#[from] UnixTimestampErr),
    #[error("StreamKeyErr: {0}")]
    StreamKeyErr(#[from] StreamKeyErr),
}

impl From<ShortStringErr> for FileErr {
    fn from(e: ShortStringErr) -> Self {
        FileErr::FormatErr(e.into())
    }
}

impl From<UnixTimestampErr> for FileErr {
    fn from(e: UnixTimestampErr) -> Self {
        FileErr::FormatErr(e.into())
    }
}

impl From<StreamKeyErr> for FileErr {
    fn from(e: StreamKeyErr) -> Self {
        FileErr::FormatErr(e.into())
    }
}

impl HeaderV1 {
    pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
        let bytes = Bytes::read_from(file, 3).await?.bytes();
        if bytes[0] != 0x53 {
            return Err(FileErr::HeaderErr(HeaderErr::ByteMark));
        }
        if bytes[1] != 0x73 {
            return Err(FileErr::HeaderErr(HeaderErr::ByteMark));
        }
        if bytes[2] != 0x01 {
            return Err(FileErr::HeaderErr(HeaderErr::Version));
        }
        let file_name = ShortString::read_from(file).await?.string();
        let created_at = UnixTimestamp::read_from(file).await?.0;
        let beacon_interval = U16::read_from(file).await?.0;
        let ret = Self {
            file_name,
            created_at,
            beacon_interval,
        };
        let _padding = Bytes::read_from(file, ret.padding_size()).await?;
        Ok(ret)
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        Bytes::Byte(0x53).write_to(file)?;
        Bytes::Byte(0x73).write_to(file)?;
        Bytes::Byte(0x01).write_to(file)?;
        let padding_size = self.padding_size();
        ShortString::new(self.file_name)?.write_to(file)?;
        UnixTimestamp(self.created_at).write_to(file)?;
        U16(self.beacon_interval).write_to(file)?;
        Bytes::Bytes(vec![0; padding_size]).write_to(file)?;
        Ok(())
    }

    pub fn padding_size(&self) -> usize {
        128 - 3 - ShortString::size_of(&self.file_name) - UnixTimestamp::size() - U16::size()
    }
}

impl MessageHeader {
    pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
        let stream_key = StreamKey::new(ShortString::read_from(file).await?.string())?;
        let shard_id = ShardId::new(U64::read_from(file).await?.0);
        let sequence = U64::read_from(file).await?.0;
        let timestamp = UnixTimestamp::read_from(file).await?.0;

        Ok(Self(sea_streamer_types::MessageHeader::new(
            stream_key, shard_id, sequence, timestamp,
        )))
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        let h = self.0;
        ShortString::new(h.stream_key().name().to_owned())?.write_to(file)?;
        U64(h.shard_id().id()).write_to(file)?;
        U64(*h.sequence()).write_to(file)?;
        UnixTimestamp(*h.timestamp()).write_to(file)?;

        Ok(())
    }
}

mod short_string {
    use super::*;

    #[derive(Error, Debug)]
    pub enum ShortStringErr {
        #[error("String too long")]
        StringTooLong,
    }

    #[repr(transparent)]
    pub struct ShortString(String);

    impl ShortString {
        pub fn new(string: String) -> Result<Self, ShortStringErr> {
            if string.len() <= u8::MAX as usize {
                Ok(Self(string))
            } else {
                Err(ShortStringErr::StringTooLong)
            }
        }

        pub fn string(self) -> String {
            self.0
        }

        pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
            let len = Bytes::read_from(file, 1).await?.byte().unwrap();
            let bytes = Bytes::read_from(file, len as usize).await?;
            Ok(Self(
                String::from_utf8(bytes.bytes()).map_err(|e| FileErr::Utf8Error(e.utf8_error()))?,
            ))
        }

        pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
            Bytes::Byte(self.0.len() as u8).write_to(file)?;
            Bytes::Bytes(self.0.into_bytes()).write_to(file)?;
            Ok(())
        }

        pub fn size(&self) -> usize {
            1 + self.0.len()
        }

        pub fn size_of(string: &String) -> usize {
            1 + string.len()
        }
    }
}

#[derive(Error, Debug)]
pub enum UnixTimestampErr {
    #[error("Out of range")]
    OutOfRange,
}

impl UnixTimestamp {
    pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
        let ts = U64::read_from(file).await?.0;
        Ok(Self(
            Timestamp::from_unix_timestamp_nanos(ts as i128 * 1_000_000)
                .map_err(|_| UnixTimestampErr::OutOfRange)?,
        ))
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        U64((self.0.unix_timestamp_nanos() / 1_000_000)
            .try_into()
            .expect("Should not be negative"))
        .write_to(file)
    }

    pub fn size() -> usize {
        U64::size()
    }
}

#[derive(Error, Debug)]
pub enum U64Err {}

impl U64 {
    pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
        let a = Bytes::read_from(file, 4).await?.word().unwrap();
        let b = Bytes::read_from(file, 4).await?.word().unwrap();
        Ok(Self(u64::from_be_bytes([
            a[0], a[1], a[2], a[3], b[0], b[1], b[2], b[3],
        ])))
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        let bytes = self.0.to_be_bytes().to_vec();
        assert_eq!(bytes.len(), 8);
        Bytes::from_bytes(bytes).write_to(file)?;
        Ok(())
    }

    pub fn size() -> usize {
        8
    }
}

#[derive(Error, Debug)]
pub enum U32Err {}

impl U32 {
    pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
        let word: [u8; 4] = Bytes::read_from(file, 4).await?.word().unwrap();
        Ok(Self(u32::from_be_bytes(word)))
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        let bytes = self.0.to_be_bytes().to_vec();
        assert_eq!(bytes.len(), 4);
        Bytes::from_bytes(bytes).write_to(file)?;
        Ok(())
    }

    pub fn size() -> usize {
        4
    }
}

#[derive(Error, Debug)]
pub enum U16Err {}

impl U16 {
    pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
        let a = Bytes::read_from(file, 1).await?.byte().unwrap();
        let b = Bytes::read_from(file, 1).await?.byte().unwrap();
        Ok(Self(u16::from_be_bytes([a, b])))
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        let bytes = self.0.to_be_bytes().to_vec();
        assert_eq!(bytes.len(), 2);
        Bytes::from_bytes(bytes).write_to(file)?;
        Ok(())
    }

    pub fn size() -> usize {
        2
    }
}
