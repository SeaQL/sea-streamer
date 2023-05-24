//! The SeaStreamer file format is designed to be seekable.
//! It has internal checksum to ensure integrity.
//! It is a binary file format, but is readable with a plain text editor (if the payload is UTF-8).
//!
//! There is a header. Every N bytes there will be a beacon summarizing the streams so far.
//! A message can be spliced by one or more beacons.
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
//! +---~----+---+----+---+----+----~----+-----+----+------+
//! | header | size of payload | payload | checksum | 0x0D |
//! +---~----+---+----+---+----+----~----+-----+----+------+
//!
//! Message spliced:
//! +----~----+----~---+--------~-------+
//! | message | beacon | message cont'd |
//! +----~----+----~---+--------~-------+
//!
//! Beacon is:
//! +-----+------+-----+------+--------------+----~----+-----+
//! | remaining message bytes | num of items |  item   | ... |
//! +-----+------+-----+------+--------------+----~----+-----+
//!
//! Message header is same as beacon item:
//! +-------------------+--------~---------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | len of stream key | stream key chars |   shard id    |     seq no    |   timestamp   |
//! +-------------------+--------~---------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//!
//! Except that beacon item has an extra tail:
//! +---------+--------+
//! | running checksum |
//! +---------+--------+
//! ```
//!
//! All numbers are encoded in big endian.

use crate::{ByteSource, Bytes, FileErr, FileSink};
use crczoo::{calculate_crc16, crc16_cdma2000, CRC16_CDMA2000_POLY};
use sea_streamer_types::{OwnedMessage, ShardId, StreamKey, StreamKeyErr, Timestamp};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderV1 {
    pub file_name: String,
    pub created_at: Timestamp,
    pub beacon_interval: u16,
}

pub type Header = HeaderV1;

pub const HEADER_SIZE: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct MessageHeader(pub sea_streamer_types::MessageHeader);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub message: OwnedMessage,
    pub checksum: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Beacons {
    pub remaining_messages_bytes: u32,
    pub items: Vec<Beacon>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Beacon {
    pub header: sea_streamer_types::MessageHeader,
    pub running_checksum: u16,
}

pub struct RunningChecksum {
    crc: u16,
}

// `ShortString` definition inside
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
    #[error("TooManyBeacon")]
    TooManyBeacon,
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
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
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

    pub fn size() -> usize {
        HEADER_SIZE
    }

    pub fn padding_size(&self) -> usize {
        HEADER_SIZE
            - 3
            - ShortString::size_of(&self.file_name)
            - UnixTimestamp::size()
            - U16::size()
    }
}

impl Message {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let header = MessageHeader::read_from(file).await?.0;
        let size = U32::read_from(file).await?.0;
        let payload = Bytes::read_from(file, size as usize).await?.bytes();
        let checksum = U16::read_from(file).await?.0;
        let message = OwnedMessage::new(header, payload);
        let _byte = Bytes::read_from(file, 1).await?.byte().unwrap();
        Ok(Self { message, checksum })
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        let (header, payload) = self.message.take();
        MessageHeader(header).write_to(file)?;
        let size = payload.len().try_into().expect("Message too big");
        U32(size).write_to(file)?;
        let checksum = crc16_cdma2000(&payload);
        Bytes::Bytes(payload).write_to(file)?;
        U16(checksum).write_to(file)?;
        Bytes::Byte(0x0D).write_to(file)?;
        Ok(())
    }
}

impl Beacons {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let remaining_messages_bytes = U32::read_from(file).await?.0;
        let mut items = Vec::new();
        let num = Bytes::read_from(file, 1).await?.byte().unwrap();
        for _ in 0..num {
            items.push(Beacon::read_from(file).await?);
        }
        Ok(Self {
            remaining_messages_bytes,
            items,
        })
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        if self.items.len() > u8::MAX as usize {
            return Err(FileErr::FormatErr(FormatErr::TooManyBeacon));
        }
        U32(self.remaining_messages_bytes).write_to(file)?;
        Bytes::Byte(self.items.len().try_into().unwrap()).write_to(file)?;
        for item in self.items {
            item.write_to(file)?;
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        let mut size = U32::size() + 1;
        for item in self.items.iter() {
            size += item.size();
        }
        size
    }

    /// Calculate the maximum number of beacons that can be fitted in the given space
    pub fn max_beacons(space: usize) -> usize {
        if space < 5 {
            return 0;
        }
        std::cmp::min(u8::MAX as usize, (space - 4 - 1) / Beacon::max_size())
    }

    /// The reasonable number of beacons to use, given the beacon_interval
    pub fn num_beacons(beacon_interval: usize) -> usize {
        Self::max_beacons(beacon_interval) / 2
    }
}

impl Beacon {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let header = MessageHeader::read_from(file).await?.0;
        let running_checksum = U16::read_from(file).await?.0;
        Ok(Self {
            header,
            running_checksum,
        })
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        MessageHeader(self.header).write_to(file)?;
        U16(self.running_checksum).write_to(file)?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        MessageHeader::size_of(&self.header) + U16::size()
    }

    pub fn max_size() -> usize {
        MessageHeader::max_size() + 2
    }
}

impl MessageHeader {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        use sea_streamer_types::MessageHeader as Header;
        let stream_key = StreamKey::new(ShortString::read_from(file).await?.string())?;
        let shard_id = ShardId::new(U64::read_from(file).await?.0);
        let sequence = U64::read_from(file).await?.0;
        let timestamp = UnixTimestamp::read_from(file).await?.0;
        Ok(Self(Header::new(stream_key, shard_id, sequence, timestamp)))
    }

    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        let h = self.0;
        ShortString::new(h.stream_key().name().to_owned())?.write_to(file)?;
        U64(h.shard_id().id()).write_to(file)?;
        U64(*h.sequence()).write_to(file)?;
        UnixTimestamp(*h.timestamp()).write_to(file)?;
        Ok(())
    }

    pub fn size_of(header: &sea_streamer_types::MessageHeader) -> usize {
        ShortString::size_of(header.stream_key().name())
            + U64::size()
            + U64::size()
            + UnixTimestamp::size()
    }

    pub fn max_size() -> usize {
        ShortString::max_size() + U64::size() + U64::size() + UnixTimestamp::size()
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
    pub struct ShortString(String); // I want to hide the inner String

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

        pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
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

        pub fn size_of(string: &str) -> usize {
            1 + string.len()
        }

        pub fn max_size() -> usize {
            1 + u8::MAX as usize
        }
    }
}

#[derive(Error, Debug)]
pub enum UnixTimestampErr {
    #[error("Out of range")]
    OutOfRange,
}

impl UnixTimestamp {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
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

/// CRC16/CDMA2000
impl RunningChecksum {
    pub fn new() -> Self {
        Self { crc: 0xFFFF }
    }

    pub fn update(&mut self, byte: u8) {
        self.crc = calculate_crc16(&[byte], CRC16_CDMA2000_POLY, self.crc, false, false, 0);
    }

    pub fn crc(&self) -> u16 {
        self.crc
    }
}

impl Default for RunningChecksum {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Error, Debug)]
pub enum U64Err {}

impl U64 {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
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
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
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
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
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

#[cfg(test)]
mod test {
    use sea_streamer_types::Buffer;

    use super::*;

    #[test]
    fn test_running_checksum() {
        let mut checksum = RunningChecksum::new();
        checksum.update(b'1');
        checksum.update(b'2');
        checksum.update(b'3');
        checksum.update(b'4');
        checksum.update(b'5');
        checksum.update(b'6');
        checksum.update(b'7');
        checksum.update(b'8');
        checksum.update(b'9');
        assert_eq!(checksum.crc(), 0x4C06);
        checksum.update(b'a');
        checksum.update(b'b');
        checksum.update(b'c');
        checksum.update(b'd');
        assert_eq!(checksum.crc(), 0xA106);
        assert_eq!(
            checksum.crc(),
            crc16_cdma2000(&"123456789abcd".into_bytes())
        );
    }

    #[test]
    fn test_num_beacons() {
        // we can only fit 1 beacon in 1kb
        assert_eq!(Beacons::num_beacons(1024), 1);
    }
}
