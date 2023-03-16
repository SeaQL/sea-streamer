use crate::{RedisErr, RedisResult, MSG, ZERO};
use redis::Value;
use sea_streamer_types::{
    MessageHeader, SeqNo, ShardId, SharedMessage, StreamErr, StreamKey, Timestamp,
};

pub type MessageId = (u64, u16);

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StreamReadReply(pub(crate) Vec<SharedMessage>);

/// The Redis message id comprises two 64 bit integers. In order to fit it into 64 bit,
/// we only allocate 48 bit to the timestamp, and the remaining 16 bit to the sub-sequence number.
///
/// This limits the number of messages per millisecond to 65536,
/// and the maximum timestamp to 10889-08-02T05:31:50.
pub fn parse_message_id(id: &str) -> RedisResult<(Timestamp, SeqNo)> {
    if let Some((timestamp, seq_no)) = id.split_once('-') {
        if let Ok(timestamp) = timestamp.parse::<u64>() {
            if let Ok(seq_no) = seq_no.parse::<u64>() {
                if seq_no > 0xFFFF {
                    return Err(StreamErr::Backend(RedisErr::MessageId(format!(
                        "Sequence number out of range: {seq_no}"
                    ))));
                }
                return Ok((
                    Timestamp::from_unix_timestamp_nanos(timestamp as i128 * 1_000_000)
                        .expect("from_unix_timestamp_nanos"),
                    timestamp << 16 | seq_no,
                ));
            }
        }
    }
    Err(StreamErr::Backend(RedisErr::MessageId(id.to_owned())))
}

pub fn get_message_id(header: &MessageHeader) -> MessageId {
    (
        (header.timestamp().unix_timestamp_nanos() / 1_000_000)
            .try_into()
            .expect("RedisConsumer: timestamp out of range"),
        (header.sequence() & 0xFFFF)
            .try_into()
            .expect("Never fails"),
    )
}

// bulk(bulk(string-data('"my_stream_1"'), bulk(bulk(string-data('"1678280595282-0"'), bulk(string-data('"msg"'), string-data('"hi 0"'), field, value, ...)), ...)))
// LOL such nesting. This is still undesirable, as there are 5 layers of nested Vec. But at least we don't have to copy the bytes again.
impl StreamReadReply {
    /// Like [`redis::FromRedisValue`], but taking ownership instead of copying.
    pub(crate) fn from_redis_value(value: Value) -> RedisResult<Self> {
        let mut messages = Vec::new();

        let err = |value| StreamErr::Backend(RedisErr::StreamReadReply(format!("{value:?}")));

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
                        for value in values {
                            if let Value::Bulk(values) = value {
                                if values.len() != 2 {
                                    return Err(err(values));
                                }
                                let mut values = values.into_iter();
                                let value_0 = values.next().unwrap();
                                let value_1 = values.next().unwrap();
                                let id = string_from_redis_value(value_0)?;
                                let (timestamp, sequence) = parse_message_id(&id)?;
                                if let Value::Bulk(values) = value_1 {
                                    assert!(values.len() % 2 == 0);
                                    let pairs = values.len() / 2;
                                    let mut values = values.into_iter();
                                    for _ in 0..pairs {
                                        let field = values.next().unwrap();
                                        let field = string_from_redis_value(field)?;
                                        let value = values.next().unwrap();
                                        if field == MSG {
                                            let bytes = bytes_from_redis_value(value)?;
                                            let length = bytes.len();
                                            messages.push(SharedMessage::new(
                                                MessageHeader::new(
                                                    stream_key.clone(),
                                                    shard,
                                                    sequence,
                                                    timestamp,
                                                ),
                                                bytes,
                                                0,
                                                length,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(StreamReadReply(messages))
    }
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
