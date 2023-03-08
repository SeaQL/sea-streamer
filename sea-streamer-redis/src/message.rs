use redis::{from_redis_value, ErrorKind, FromRedisValue, RedisError, RedisResult};
use sea_streamer_types::{MessageHeader, SeqNo, SharedMessage, StreamKey, Timestamp};

pub use redis::Value;

use crate::{RedisErr, MSG, ZERO};

/// The Redis message id comprises two 64 bit integers. In order to fit it into 64 bit,
/// we only allocate 48 bit to the timestamp, and the remaining 16 bit to the sub-sequence number.
///
/// This limits the number of messages per millisecond to 65536,
/// and the maximum timestamp to 10889-08-02T05:31:50.
pub fn parse_message_id(id: &str) -> Result<(Timestamp, SeqNo), RedisErr> {
    if let Some((timestamp, seq_no)) = id.split_once('-') {
        if let Ok(timestamp) = timestamp.parse::<u64>() {
            if let Ok(seq_no) = seq_no.parse::<u64>() {
                return Ok((
                    Timestamp::from_unix_timestamp_nanos(timestamp as i128 * 1_000_000)
                        .expect("from_unix_timestamp_nanos"),
                    timestamp << 16 | seq_no,
                ));
            }
        }
    }
    Err(RedisErr::MessageId(id.to_owned()))
}

#[derive(Debug)]
pub struct StreamReadReply {
    pub messages: Vec<SharedMessage>,
}

// bulk(bulk(string-data('"my_stream_1"'), bulk(bulk(string-data('"1678280595282-0"'), bulk(string-data('"msg"'), string-data('"hi 0"'), field, value, ...)), ...)))
impl FromRedisValue for StreamReadReply {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        let mut messages = Vec::new();

        let err = || {
            let err: RedisError =
                (ErrorKind::ResponseError, "Failed to parse StreamReadReply").into();
            err
        };

        match value {
            Value::Bulk(values) => {
                for value in values {
                    match value {
                        Value::Bulk(values) => {
                            if values.len() != 2 {
                                return Err(err());
                            }
                            let stream_key: String = from_redis_value(&values[0])?;
                            let stream_key = StreamKey::new(stream_key).map_err(|_| {
                                let err: RedisError =
                                    (ErrorKind::ResponseError, "Invalid StreamKey").into();
                                err
                            })?;
                            match &values[1] {
                                Value::Bulk(values) => {
                                    for value in values {
                                        match value {
                                            Value::Bulk(values) => {
                                                if values.len() != 2 {
                                                    return Err(err());
                                                }
                                                let id: String = from_redis_value(&values[0])?;
                                                let (timestamp, sequence) =
                                                    parse_message_id(&id).map_err(|_| err())?;
                                                match &values[1] {
                                                    Value::Bulk(values) => {
                                                        for pair in values.chunks(2) {
                                                            assert_eq!(pair.len(), 2);
                                                            let field: String =
                                                                from_redis_value(&pair[0])?;
                                                            let value = &pair[1];
                                                            if field == MSG {
                                                                let bytes: Vec<u8> =
                                                                    from_redis_value(&value)?;
                                                                let length = bytes.len();
                                                                messages.push(SharedMessage::new(
                                                                    MessageHeader::new(
                                                                        stream_key.clone(),
                                                                        ZERO,
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
                                                    _ => (),
                                                }
                                            }
                                            _ => (),
                                        }
                                    }
                                }
                                _ => (),
                            }
                        }
                        _ => (),
                    }
                }
            }
            _ => (),
        }

        Ok(StreamReadReply { messages })
    }
}
