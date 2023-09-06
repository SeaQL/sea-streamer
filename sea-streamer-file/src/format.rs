//! The SeaStreamer file format is a container format designed to be seekable.
//! It does not concerns what format the payload is encoded in.
//! It has internal checksum to ensure integrity.
//! It is a binary file format, but is readable with a plain text editor (if the payload is UTF-8).
//!
//! There is a header. Every N bytes there will be a Beacon summarizing the streams so far.
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
//! +--------+--------+---------+---~---+----~----+------+
//! |  0x53  |  0x73  | version | meta  | padding | 0x0D |
//! +--------+--------+---------+---~---+----~----+------+
//!
//! Header meta v1 is always 128 - 3 bytes long. Padding is stuffed with 0, ending with a \n.
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
//! +------+-----+------+-----+------+--------------+----~----+-----+------+
//! | 0x0D | remaining message bytes | num of items |  item   | ... | 0x0D |
//! +------+-----+------+-----+------+--------------+----~----+-----+------+
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
//! All numbers are encoded in big endian. There are 0x0D in places so that it will not blow up
//! plain text editors. And it's semi-human-readable.
//!
//! A SeaStreamer file can be terminated by a End-of-Stream Message,
//! with the stream key `SEA_STREAMER_INTERNAL` and payload `EOS`.

use crate::{
    crc::{crc16_cdma2000, crc_update},
    ByteSink, ByteSource, Bytes, FileErr,
};
use sea_streamer_types::{
    Buffer, Message as MessageTrait, OwnedMessage, ShardId, StreamKey, StreamKeyErr, Timestamp,
};
#[cfg(feature = "serde")]
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct HeaderV1 {
    pub file_name: String,
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize_timestamp"))]
    pub created_at: Timestamp,
    pub beacon_interval: u32,
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

#[cfg(feature = "serde_json")]
#[derive(Serialize)]
pub struct MessageJson<'a> {
    pub header: &'a sea_streamer_types::MessageHeader,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Beacon {
    pub remaining_messages_bytes: u32,
    pub items: Vec<Marker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Marker {
    pub header: sea_streamer_types::MessageHeader,
    pub running_checksum: Checksum,
}

pub struct RunningChecksum {
    crc: u16,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[repr(transparent)]
pub struct Checksum(pub u16);

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

#[derive(Error, Debug, Clone, Copy)]
pub enum FormatErr {
    #[error("Byte mark mismatch")]
    ByteMark,
    #[error("Version mismatch")]
    Version,
    #[error("ShortStringErr: {0}")]
    ShortStringErr(#[from] ShortStringErr),
    #[error("UnixTimestampErr: {0}")]
    UnixTimestampErr(#[from] UnixTimestampErr),
    #[error("StreamKeyErr: {0}")]
    StreamKeyErr(#[from] StreamKeyErr),
    #[error("TooManyBeacon")]
    TooManyBeacon,
    #[error("Checksum error: received {received}, computed {computed}")]
    ChecksumErr { received: u16, computed: u16 },
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
            return Err(FileErr::FormatErr(FormatErr::ByteMark));
        }
        if bytes[1] != 0x73 {
            return Err(FileErr::FormatErr(FormatErr::ByteMark));
        }
        if bytes[2] != 0x01 {
            return Err(FileErr::FormatErr(FormatErr::Version));
        }
        let file_name = ShortString::read_from(file).await?.string();
        let created_at = UnixTimestamp::read_from(file).await?.0;
        let beacon_interval = U32::read_from(file).await?.0;
        let ret = Self {
            file_name,
            created_at,
            beacon_interval,
        };
        let _padding = Bytes::read_from(file, ret.padding_size()).await?;
        Ok(ret)
    }

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        let mut sum = 0;
        sum += Bytes::Byte(0x53).write_to(sink)?;
        sum += Bytes::Byte(0x73).write_to(sink)?;
        sum += Bytes::Byte(0x01).write_to(sink)?;
        let padding_size = self.padding_size();
        sum += ShortString::new(self.file_name)?.write_to(sink)?;
        sum += UnixTimestamp(self.created_at).write_to(sink)?;
        sum += U32(self.beacon_interval).write_to(sink)?;
        sum += Bytes::Bytes(vec![0; padding_size - 1]).write_to(sink)?;
        sum += Bytes::Byte(0x0D).write_to(sink)?;
        Ok(sum)
    }

    #[inline]
    pub const fn size() -> usize {
        HEADER_SIZE
    }

    pub fn padding_size(&self) -> usize {
        HEADER_SIZE
            - 3
            - ShortString::size_of(&self.file_name)
            - UnixTimestamp::size()
            - U32::size()
    }
}

impl Message {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let header = MessageHeader::read_from(file).await?.0;
        let size = U32::read_from(file).await?.0;
        let payload = Bytes::read_from(file, size as usize).await?.bytes();
        let checksum = U16::read_from(file).await?.0;
        let message = OwnedMessage::new(header, payload);
        _ = Bytes::read_from(file, 1).await?.byte().unwrap();
        Ok(Self { message, checksum })
    }

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<(usize, Checksum), FileErr> {
        let mut sum = 0;
        let (header, payload) = self.message.take();
        sum += MessageHeader(header).write_to(sink)?;
        let size = payload.len().try_into().expect("Message too big");
        sum += U32(size).write_to(sink)?;
        let checksum = crc16_cdma2000(&payload);
        sum += Bytes::Bytes(payload).write_to(sink)?;
        sum += U16(checksum).write_to(sink)?;
        sum += Bytes::Byte(0x0D).write_to(sink)?;
        Ok((sum, Checksum(checksum)))
    }

    pub fn size(&self) -> usize {
        MessageHeader::size_of(self.message.header())
            + U32::size()
            + self.message.message().size()
            + U16::size()
            + 1
    }

    pub fn compute_checksum(&self) -> u16 {
        crc16_cdma2000(self.message.message().as_bytes())
    }
}

impl Beacon {
    pub fn empty() -> Self {
        Self {
            remaining_messages_bytes: 0,
            items: Vec::new(),
        }
    }

    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        _ = Bytes::read_from(file, 1).await?;
        let remaining_messages_bytes = U32::read_from(file).await?.0;
        let mut items = Vec::new();
        let num = Bytes::read_from(file, 1).await?.byte().unwrap();
        for _ in 0..num {
            items.push(Marker::read_from(file).await?);
        }
        _ = Bytes::read_from(file, 1).await?;
        Ok(Self {
            remaining_messages_bytes,
            items,
        })
    }

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        let mut sum = 0;
        sum += Bytes::Byte(0x0D).write_to(sink)?;
        if self.items.len() > u8::MAX as usize {
            return Err(FileErr::FormatErr(FormatErr::TooManyBeacon));
        }
        sum += U32(self.remaining_messages_bytes).write_to(sink)?;
        sum += Bytes::Byte(self.items.len().try_into().unwrap()).write_to(sink)?;
        for item in self.items {
            sum += item.write_to(sink)?;
        }
        sum += Bytes::Byte(0x0D).write_to(sink)?;
        Ok(sum)
    }

    pub fn size(&self) -> usize {
        let mut size = 1 + U32::size() + 1;
        for item in self.items.iter() {
            size += item.size();
        }
        size + 1
    }

    /// Calculate the maximum number of markers that can be fitted in the given space
    pub fn max_markers(space: usize) -> usize {
        if space < 7 {
            return 0;
        }
        std::cmp::min(u8::MAX as usize, (space - 7) / Marker::max_size())
    }

    /// The reasonable number of markers to use, given the beacon_interval
    pub fn num_markers(beacon_interval: usize) -> usize {
        Self::max_markers(beacon_interval) / 2
    }
}

impl Marker {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let header = MessageHeader::read_from(file).await?.0;
        let running_checksum = Checksum(U16::read_from(file).await?.0);
        Ok(Self {
            header,
            running_checksum,
        })
    }

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        let mut sum = 0;
        sum += MessageHeader(self.header).write_to(sink)?;
        sum += U16(self.running_checksum.0).write_to(sink)?;
        Ok(sum)
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

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        let mut sum = 0;
        let h = self.0;
        sum += ShortString::new(h.stream_key().name().to_owned())?.write_to(sink)?;
        sum += U64(h.shard_id().id()).write_to(sink)?;
        sum += U64(*h.sequence()).write_to(sink)?;
        sum += UnixTimestamp(*h.timestamp()).write_to(sink)?;
        Ok(sum)
    }

    pub fn size(&self) -> usize {
        Self::size_of(&self.0)
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

    #[derive(Error, Debug, Clone, Copy)]
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

        pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
            let mut sum = 0;
            sum += Bytes::Byte(self.0.len() as u8).write_to(sink)?;
            sum += Bytes::Bytes(self.0.into_bytes()).write_to(sink)?;
            Ok(sum)
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

#[derive(Error, Debug, Clone, Copy)]
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

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        U64((self.0.unix_timestamp_nanos() / 1_000_000)
            .try_into()
            .expect("Should not be negative"))
        .write_to(sink)
    }

    pub fn size() -> usize {
        U64::size()
    }
}

/// CRC16/CDMA2000
impl RunningChecksum {
    pub fn new() -> Self {
        Self { crc: Self::init() }
    }

    #[inline]
    pub fn init() -> u16 {
        0xFFFF
    }

    pub fn resume(crc: Checksum) -> Self {
        Self { crc: crc.0 }
    }

    pub fn update(&mut self, checksum: Checksum) {
        let bytes = checksum.0.to_be_bytes();
        self.byte(bytes[0]);
        self.byte(bytes[1]);
    }

    fn byte(&mut self, byte: u8) {
        self.crc = crc_update(self.crc, &[byte]);
    }

    pub fn crc(&self) -> Checksum {
        Checksum(self.crc)
    }
}

impl Default for RunningChecksum {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Error, Debug, Clone, Copy)]
pub enum U64Err {}

impl U64 {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let a = Bytes::read_from(file, 4).await?.word().unwrap();
        let b = Bytes::read_from(file, 4).await?.word().unwrap();
        Ok(Self(u64::from_be_bytes([
            a[0], a[1], a[2], a[3], b[0], b[1], b[2], b[3],
        ])))
    }

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        let bytes = self.0.to_be_bytes().to_vec();
        assert_eq!(bytes.len(), 8);
        Bytes::from_bytes(bytes).write_to(sink)
    }

    pub fn size() -> usize {
        8
    }
}

#[derive(Error, Debug, Clone, Copy)]
pub enum U32Err {}

impl U32 {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let word: [u8; 4] = Bytes::read_from(file, 4).await?.word().unwrap();
        Ok(Self(u32::from_be_bytes(word)))
    }

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        let bytes = self.0.to_be_bytes().to_vec();
        assert_eq!(bytes.len(), 4);
        Bytes::from_bytes(bytes).write_to(sink)
    }

    pub fn size() -> usize {
        4
    }
}

#[derive(Error, Debug, Clone, Copy)]
pub enum U16Err {}

impl U16 {
    pub async fn read_from(file: &mut impl ByteSource) -> Result<Self, FileErr> {
        let a = Bytes::read_from(file, 1).await?.byte().unwrap();
        let b = Bytes::read_from(file, 1).await?.byte().unwrap();
        Ok(Self(u16::from_be_bytes([a, b])))
    }

    pub fn write_to(self, sink: &mut impl ByteSink) -> Result<usize, FileErr> {
        let bytes = self.0.to_be_bytes().to_vec();
        assert_eq!(bytes.len(), 2);
        Bytes::from_bytes(bytes).write_to(sink)
    }

    pub fn size() -> usize {
        2
    }
}

#[cfg(feature = "serde")]
pub fn serialize_timestamp<S>(ts: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(
        ts.format(sea_streamer_types::TIMESTAMP_FORMAT)
            .unwrap()
            .as_str(),
    )
}

#[cfg(test)]
mod test {
    use sea_streamer_types::Buffer;

    use super::*;

    #[test]
    fn test_running_checksum() {
        let mut checksum = RunningChecksum::new();
        checksum.byte(b'1');
        checksum.byte(b'2');
        checksum.byte(b'3');
        checksum.byte(b'4');
        checksum.byte(b'5');
        checksum.byte(b'6');
        checksum.byte(b'7');
        checksum.byte(b'8');
        checksum.byte(b'9');
        assert_eq!(checksum.crc(), Checksum(0x4C06));
        checksum.byte(b'a');
        checksum.byte(b'b');
        checksum.byte(b'c');
        checksum.byte(b'd');
        assert_eq!(checksum.crc(), Checksum(0xA106));
        assert_eq!(
            checksum.crc().0,
            crc16_cdma2000(&"123456789abcd".into_bytes())
        );
    }

    #[test]
    fn test_num_markers() {
        assert_eq!(Beacon::num_markers(640), 1);
        // we can only fit 1 marker in 1kb
        assert_eq!(Beacon::num_markers(1024), 1);
    }
}
