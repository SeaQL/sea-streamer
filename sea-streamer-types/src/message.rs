use std::{str::Utf8Error, sync::Arc};

use crate::{SeqNo, ShardId, StreamKey, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnedMessage {
    header: MessageHeader,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// It uses an `Arc` to hold the bytes, so is cheap to clone.
pub struct SharedMessage {
    header: MessageHeader,
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
pub trait Message: Send {
    fn stream_key(&self) -> StreamKey;

    fn shard_id(&self) -> ShardId;

    fn sequence(&self) -> SeqNo;

    fn timestamp(&self) -> Timestamp;

    fn message(&self) -> Payload;

    fn to_owned(&self) -> SharedMessage {
        SharedMessage::new(
            MessageHeader::new(
                self.stream_key(),
                self.shard_id(),
                self.sequence(),
                self.timestamp(),
            ),
            self.message().into_bytes(),
            0,
            self.message().size(),
        )
    }

    /// tuple to uniquely identify a message
    fn identifier(&self) -> (StreamKey, ShardId, SeqNo) {
        (self.stream_key(), self.shard_id(), self.sequence())
    }
}

impl OwnedMessage {
    pub fn new(header: MessageHeader, payload: Vec<u8>) -> Self {
        Self { header, payload }
    }

    pub fn header(&self) -> &MessageHeader {
        &self.header
    }

    pub fn take(self) -> (MessageHeader, Vec<u8>) {
        let Self { header, payload } = self;
        (header, payload)
    }

    pub fn to_shared(self) -> SharedMessage {
        let (header, payload) = self.take();
        let size = payload.len();
        SharedMessage::new(header, payload, 0, size)
    }
}

impl SharedMessage {
    pub fn new(header: MessageHeader, bytes: Vec<u8>, offset: usize, length: usize) -> Self {
        assert!(offset <= bytes.len());
        Self {
            header,
            bytes: Arc::new(bytes),
            offset: offset as u32,
            length: length as u32,
        }
    }

    /// Touch the timestamp to now
    pub fn touch(&mut self) {
        self.header.timestamp = Timestamp::now_utc();
    }

    pub fn header(&self) -> &MessageHeader {
        &self.header
    }

    pub fn take_header(self) -> MessageHeader {
        self.header
    }

    /// This will attempt to convert self into an OwnedMessage *without* copying,
    /// if the bytes are not shared with any other.
    pub fn to_owned_message(self) -> OwnedMessage {
        let payload = if self.offset == 0 && self.length as usize == self.bytes.len() {
            Arc::try_unwrap(self.bytes).unwrap_or_else(|arc| (*arc).clone())
        } else {
            self.message().into_bytes()
        };
        OwnedMessage {
            header: self.header,
            payload,
        }
    }
}

impl Message for OwnedMessage {
    fn stream_key(&self) -> StreamKey {
        self.header.stream_key().clone()
    }

    fn shard_id(&self) -> ShardId {
        *self.header.shard_id()
    }

    fn sequence(&self) -> SeqNo {
        *self.header.sequence()
    }

    fn timestamp(&self) -> Timestamp {
        *self.header.timestamp()
    }

    fn message(&self) -> Payload {
        Payload {
            data: BytesOrStr::Bytes(&self.payload),
        }
    }
}

impl Message for SharedMessage {
    fn stream_key(&self) -> StreamKey {
        self.header.stream_key().clone()
    }

    fn shard_id(&self) -> ShardId {
        *self.header.shard_id()
    }

    fn sequence(&self) -> SeqNo {
        *self.header.sequence()
    }

    fn timestamp(&self) -> Timestamp {
        *self.header.timestamp()
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

impl Buffer for Payload<'_> {
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

impl Buffer for &'_ [u8] {
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

impl Buffer for &'_ str {
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

impl Default for Payload<'_> {
    fn default() -> Self {
        Self::new("")
    }
}

impl BytesOrStr<'_> {
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

#[cfg(feature = "serde")]
impl serde::Serialize for MessageHeader {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(serde::Serialize)]
        struct HeaderJson<'a> {
            stream_key: &'a str,
            shard_id: u64,
            sequence: SeqNo,
            timestamp: String,
        }

        HeaderJson {
            timestamp: self
                .timestamp
                .format(crate::TIMESTAMP_FORMAT)
                .expect("Timestamp format error"),
            stream_key: self.stream_key.name(),
            sequence: self.sequence,
            shard_id: self.shard_id.id(),
        }
        .serialize(serializer)
    }
}
