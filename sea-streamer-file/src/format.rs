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
use sea_streamer_types::Timestamp;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderV1 {
    pub file_name: String,
    pub created_at: Timestamp,
}

pub type Header = HeaderV1;

#[repr(transparent)]
pub struct ShortString(String);

/// Timestamp in seconds
#[repr(transparent)]
pub struct UnixTimestamp(Timestamp);

#[repr(transparent)]
pub struct U64(u64);

#[derive(Error, Debug)]
pub enum HeaderErr {
    #[error("Byte mark mismatch")]
    ByteMark,
    #[error("Version mismatch")]
    Version,
}

#[derive(Error, Debug)]
pub enum FormatErr {
    #[error("U64Err: {0}")]
    U64Err(#[from] U64Err),
    #[error("ShortStringErr: {0}")]
    ShortStringErr(#[from] ShortStringErr),
    #[error("UnixTimestampErr: {0}")]
    UnixTimestampErr(#[from] UnixTimestampErr),
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
        let file_name = ShortString::read_from(file).await?.0;
        let created_at = UnixTimestamp::read_from(file).await?.0;
        let ret = Self {
            file_name,
            created_at,
        };
        let _padding = Bytes::read_from(file, ret.padding_size()).await?;
        Ok(ret)
    }

    pub fn padding_size(&self) -> usize {
        128 - 3 - ShortString::size_of(&self.file_name) - UnixTimestamp::size()
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        Bytes::Byte(0x53).write_to(file)?;
        Bytes::Byte(0x73).write_to(file)?;
        Bytes::Byte(0x01).write_to(file)?;
        let padding_size = self.padding_size();
        ShortString::new(self.file_name)
            .map_err(|e| FileErr::FormatErr(FormatErr::ShortStringErr(e)))?
            .write_to(file)?;
        UnixTimestamp(self.created_at).write_to(file)?;
        Bytes::Bytes(vec![0; padding_size]).write_to(file)?;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum ShortStringErr {
    #[error("String too long")]
    StringTooLong,
}

impl ShortString {
    pub fn new(string: String) -> Result<Self, ShortStringErr> {
        if string.len() <= u8::MAX as usize {
            Ok(Self(string))
        } else {
            Err(ShortStringErr::StringTooLong)
        }
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

#[derive(Error, Debug)]
pub enum UnixTimestampErr {
    #[error("Out of range")]
    OutOfRange,
}

impl UnixTimestamp {
    pub async fn read_from(file: &mut FileSource) -> Result<Self, FileErr> {
        let ts = U64::read_from(file).await?.0;
        Ok(Self(
            Timestamp::from_unix_timestamp(ts.try_into().expect("Out of range")).map_err(|_| {
                FileErr::FormatErr(FormatErr::UnixTimestampErr(UnixTimestampErr::OutOfRange))
            })?,
        ))
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        U64(self
            .0
            .unix_timestamp()
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
