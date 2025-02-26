use crate::{MessageField as MsgF, RedisErr, RedisResult, TimestampFormat as TsFmt, ZERO};
use redis::Value;
use sea_streamer_types::{
    MessageHeader, SeqNo, ShardId, SharedMessage, StreamErr, StreamKey, Timestamp,
};

/// ID of a message in the form of (timestamp, sequence).
pub type MessageId = (u64, u16);

/// To indicate `$`, aka latest.
pub const MAX_MSG_ID: MessageId = (u64::MAX, u16::MAX);

pub type RedisMessage = SharedMessage;

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StreamReadReply(pub(crate) Vec<RedisMessage>);

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StreamRangeReply(pub(crate) Vec<RedisMessage>);

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct AutoClaimReply(pub(crate) Vec<RedisMessage>);

/// The Redis message id comprises two 64 bit integers. In order to fit it into 64 bit,
/// we only allocate 48 bit to the timestamp, and the remaining 16 bit to the sub-sequence number.
///
/// This limits the number of messages per millisecond to 65536,
/// and the maximum timestamp to `10889-08-02T05:31:50`.
///
/// This limit can be lifted with the `nanosecond-timestamp` feature flag, which widens
/// SeaStreamer's SeqNo to be u128. But squeezing nanosecond timestamps into Redis still
/// limit it to `2554-07-21T23:34:33`.
pub fn parse_message_id(ts_fmt: TsFmt, id: &str) -> RedisResult<(Timestamp, SeqNo)> {
    if let Some((timestamp, seq_no)) = id.split_once('-') {
        if let Ok(timestamp) = timestamp.parse::<u64>() {
            if let Ok(seq_no) = seq_no.parse::<u64>() {
                if seq_no > 0xFFFF {
                    return Err(StreamErr::Backend(RedisErr::MessageId(format!(
                        "Sequence number out of range: {seq_no}"
                    ))));
                }
                #[cfg(not(feature = "nanosecond-timestamp"))]
                if timestamp > 0xFFFFFFFFFFFF {
                    return Err(StreamErr::Backend(RedisErr::MessageId(format!(
                        "Timestamp out of range: {timestamp}"
                    ))));
                }
                let nano = match ts_fmt {
                    TsFmt::UnixTimestampMillis => timestamp as i128 * 1_000_000,
                    #[cfg(feature = "nanosecond-timestamp")]
                    TsFmt::UnixTimestampNanos => timestamp as i128,
                };
                return Ok((
                    Timestamp::from_unix_timestamp_nanos(nano).unwrap(),
                    (timestamp as SeqNo) << 16 | seq_no as SeqNo,
                ));
            }
        }
    }
    Err(StreamErr::Backend(RedisErr::MessageId(id.to_owned())))
}

#[inline]
pub(crate) fn get_message_id(header: &MessageHeader) -> MessageId {
    from_seq_no(*header.sequence())
}

#[inline]
pub(crate) fn from_seq_no(seq_no: SeqNo) -> MessageId {
    #[allow(clippy::unnecessary_cast)]
    ((seq_no >> 16) as u64, (seq_no & 0xFFFF) as u16)
}

/// A trait that adds some methods to [`RedisMessage`].
pub trait RedisMessageId {
    /// Get the Redis MessageId in form of (timestamp,seq) tuple from the message
    fn message_id(&self) -> MessageId;
}

impl RedisMessageId for RedisMessage {
    fn message_id(&self) -> MessageId {
        get_message_id(self.header())
    }
}

// bulk(bulk(string-data('"my_stream_1"'), bulk(bulk(string-data('"1678280595282-0"'), bulk(string-data('"msg"'), string-data('"hi 0"'), field, value, ...)), ...)))
// LOL such nesting. This is still undesirable, as there are 5 layers of nested Vec. But at least we don't have to copy the bytes again.
impl StreamReadReply {
    /// Like [`redis::FromRedisValue`], but taking ownership instead of copying.
    pub(crate) fn from_redis_value(value: Value, ts_fmt: TsFmt, msg: MsgF) -> RedisResult<Self> {
        let mut messages = Vec::new();

        if let Value::Bulk(values) = value {
            for value in values {
                if let Value::Bulk(values) = value {
                    if values.len() != 2 {
                        return Err(err(values));
                    }
                    let mut values = values.into_iter();
                    let value_0 = values.next().unwrap();
                    let value_1 = values.next().unwrap();
                    let stream_key = string_from_redis_value(value_0)?;
                    let (stream_key, shard) =
                        if let Some((front, remaining)) = stream_key.split_once(':') {
                            (
                                front.to_owned(),
                                ShardId::new(remaining.parse().map_err(|_| {
                                    StreamErr::Backend(RedisErr::StreamReadReply(format!(
                                        "Failed to parse `{remaining}` as u64"
                                    )))
                                })?),
                            )
                        } else {
                            (stream_key, ZERO)
                        };
                    let stream_key = StreamKey::new(stream_key)?;
                    if let Value::Bulk(values) = value_1 {
                        parse_messages(values, stream_key, shard, &mut messages, ts_fmt, msg)?;
                    }
                }
            }
        }

        Ok(Self(messages))
    }
}

impl StreamRangeReply {
    pub(crate) fn from_redis_value(
        values: Value,
        stream_key: StreamKey,
        ts_fmt: TsFmt,
        msg: MsgF,
    ) -> RedisResult<Self> {
        let mut messages = Vec::new();

        if let Value::Bulk(values) = values {
            parse_messages(values, stream_key, ZERO, &mut messages, ts_fmt, msg)?;
        }

        Ok(Self(messages))
    }
}

impl AutoClaimReply {
    pub(crate) fn from_redis_value(
        value: Value,
        stream_key: StreamKey,
        shard: ShardId,
        ts_fmt: TsFmt,
        msg: MsgF,
    ) -> RedisResult<Self> {
        let mut messages = Vec::new();
        if let Value::Bulk(values) = value {
            if values.len() != 3 {
                return Err(err(values));
            }
            let mut values = values.into_iter();
            _ = values.next().unwrap();
            let value = values.next().unwrap();
            if let Value::Bulk(values) = value {
                parse_messages(values, stream_key, shard, &mut messages, ts_fmt, msg)?;
            } else {
                return Err(err(value));
            }
        }
        Ok(Self(messages))
    }
}

pub(crate) fn parse_messages(
    values: Vec<Value>,
    stream: StreamKey,
    shard: ShardId,
    messages: &mut Vec<RedisMessage>,
    ts_fmt: TsFmt,
    msg: MsgF,
) -> RedisResult<()> {
    for value in values {
        if let Value::Bulk(values) = value {
            if values.len() != 2 {
                return Err(err(values));
            }
            let mut values = values.into_iter();
            let value_0 = values.next().unwrap();
            let value_1 = values.next().unwrap();
            let id = string_from_redis_value(value_0)?;
            let (timestamp, sequence) = parse_message_id(ts_fmt, &id)?;
            if let Value::Bulk(values) = value_1 {
                assert!(values.len() % 2 == 0);
                let pairs = values.len() / 2;
                let mut values = values.into_iter();
                for _ in 0..pairs {
                    let field = values.next().unwrap();
                    let field = string_from_redis_value(field)?;
                    let value = values.next().unwrap();
                    if field == msg.0 {
                        let bytes = bytes_from_redis_value(value)?;
                        let length = bytes.len();
                        messages.push(RedisMessage::new(
                            MessageHeader::new(stream.clone(), shard, sequence, timestamp),
                            bytes,
                            0,
                            length,
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn err<D: std::fmt::Debug>(d: D) -> StreamErr<RedisErr> {
    StreamErr::Backend(RedisErr::StreamReadReply(format!("{d:?}")))
}

pub(crate) fn string_from_redis_value(v: Value) -> RedisResult<String> {
    match v {
        Value::Data(bytes) => {
            Ok(String::from_utf8(bytes).map_err(|e| StreamErr::Utf8Error(e.utf8_error()))?)
        }
        Value::Okay => Ok("OK".to_owned()),
        Value::Status(val) => Ok(val),
        _ => Err(StreamErr::Backend(RedisErr::TypeError(
            "Expected String".to_owned(),
        ))),
    }
}

pub(crate) fn bytes_from_redis_value(v: Value) -> RedisResult<Vec<u8>> {
    match v {
        Value::Data(bytes) => Ok(bytes),
        _ => Err(StreamErr::Backend(RedisErr::TypeError(
            "Expected Data".to_owned(),
        ))),
    }
}
