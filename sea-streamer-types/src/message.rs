use std::{str::Utf8Error, sync::Arc};

use crate::{SeqNo, ShardId, StreamKey, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// It uses an `Arc` to hold the bytes, so is cheap to clone.
pub struct SharedMessage {
    meta: MessageHeader,
    bytes: Arc<Vec<u8>>,
    offset: u32,
    length: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// The payload of a message.
pub struct Payload<'a> {
    data: BytesOrStr<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Bytes or Str. Being an `str` means the data is UTF-8 valid.
pub enum BytesOrStr<'a> {
    Bytes(&'a [u8]),
    Str(&'a str),
}

/// Types that be converted into [`BytesOrStr`].
pub trait IntoBytesOrStr<'a>
where
    Self: 'a,
{
    fn into_bytes_or_str(self) -> BytesOrStr<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Metadata associated with a message.
pub struct MessageHeader {
    stream_key: StreamKey,
    shard_id: ShardId,
    sequence: SeqNo,
    timestamp: Timestamp,
}

/// Common interface of byte containers.
pub trait Buffer {
    fn size(&self) -> usize;

    fn into_bytes(self) -> Vec<u8>;

    fn as_bytes(&self) -> &[u8];

    fn as_str(&self) -> Result<&str, Utf8Error>;
}

/// Common interface of messages, to be implemented by all backends.
pub trait Message {
    fn stream_key(&self) -> StreamKey;

    fn shard_id(&self) -> ShardId;

    fn sequence(&self) -> SeqNo;

    fn timestamp(&self) -> Timestamp;

    fn message(&self) -> Payload;
}

impl SharedMessage {
    pub fn new(meta: MessageHeader, bytes: Vec<u8>, offset: usize, length: usize) -> Self {
        assert!(offset <= bytes.len());
        Self {
            meta,
            bytes: Arc::new(bytes),
            offset: offset as u32,
            length: length as u32,
        }
    }

    /// Touch the timestamp to now
    pub fn touch(&mut self) {
        self.meta.timestamp = Timestamp::now_utc();
    }

    pub fn take_meta(self) -> MessageHeader {
        self.meta
    }
}

impl Message for SharedMessage {
    fn stream_key(&self) -> StreamKey {
        self.meta.stream_key().to_owned()
    }

    fn shard_id(&self) -> ShardId {
        *self.meta.shard_id()
    }

    fn sequence(&self) -> SeqNo {
        *self.meta.sequence()
    }

    fn timestamp(&self) -> Timestamp {
        *self.meta.timestamp()
    }

    fn message(&self) -> Payload {
        Payload {
            data: BytesOrStr::Bytes(
                &self.bytes[self.offset as usize..(self.offset + self.length) as usize],
            ),
        }
    }
}

impl MessageHeader {
    pub fn new(
        stream_key: StreamKey,
        shard_id: ShardId,
        sequence: SeqNo,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            stream_key,
            shard_id,
            sequence,
            timestamp,
        }
    }

    pub fn stream_key(&self) -> &StreamKey {
        &self.stream_key
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    pub fn sequence(&self) -> &SeqNo {
        &self.sequence
    }

    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }
}

impl<'a> Buffer for Payload<'a> {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn into_bytes(self) -> Vec<u8> {
        match self.data {
            BytesOrStr::Bytes(bytes) => bytes.into_bytes(),
            BytesOrStr::Str(str) => str.into_bytes(),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        match self.data {
            BytesOrStr::Bytes(bytes) => bytes,
            BytesOrStr::Str(str) => str.as_bytes(),
        }
    }

    fn as_str(&self) -> Result<&str, Utf8Error> {
        match &self.data {
            BytesOrStr::Bytes(bytes) => bytes.as_str(),
            BytesOrStr::Str(str) => Ok(str),
        }
    }
}

impl<'a> Buffer for &'a [u8] {
    fn size(&self) -> usize {
        self.len()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.to_owned()
    }

    fn as_bytes(&self) -> &[u8] {
        self
    }

    fn as_str(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(self)
    }
}

impl<'a> Buffer for &'a str {
    fn size(&self) -> usize {
        self.len()
    }

    fn into_bytes(self) -> Vec<u8> {
        self.as_bytes().to_owned()
    }

    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }

    fn as_str(&self) -> Result<&str, Utf8Error> {
        Ok(self)
    }
}

impl Buffer for String {
    fn size(&self) -> usize {
        self.len()
    }

    fn into_bytes(self) -> Vec<u8> {
        String::into_bytes(self)
    }

    fn as_bytes(&self) -> &[u8] {
        String::as_bytes(self)
    }

    fn as_str(&self) -> Result<&str, Utf8Error> {
        Ok(self.as_str())
    }
}

impl<'a> Payload<'a> {
    pub fn new<D: IntoBytesOrStr<'a>>(data: D) -> Self {
        Self {
            data: data.into_bytes_or_str(),
        }
    }

    #[cfg(feature = "json")]
    #[cfg_attr(docsrs, doc(cfg(feature = "json")))]
    pub fn deserialize_json<D: serde::de::DeserializeOwned>(&self) -> Result<D, crate::JsonErr> {
        Ok(serde_json::from_str(self.as_str()?)?)
    }
}

impl<'a> BytesOrStr<'a> {
    pub fn len(&self) -> usize {
        match self {
            BytesOrStr::Bytes(bytes) => bytes.len(),
            BytesOrStr::Str(str) => str.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> IntoBytesOrStr<'a> for &'a str {
    fn into_bytes_or_str(self) -> BytesOrStr<'a> {
        BytesOrStr::Str(self)
    }
}

impl<'a> IntoBytesOrStr<'a> for &'a [u8] {
    fn into_bytes_or_str(self) -> BytesOrStr<'a> {
        BytesOrStr::Bytes(self)
    }
}
