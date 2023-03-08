use redis::{
    aio::ConnectionLike, cmd as command, streams::StreamReadOptions, ErrorKind, RedisError,
    RedisResult, ToRedisArgs,
};
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

pub async fn xread_options<C, K, ID>(
    con: &mut C,
    keys: &[K],
    ids: &[ID],
    options: &StreamReadOptions,
) -> RedisResult<StreamReadReply>
where
    K: ToRedisArgs,
    ID: ToRedisArgs,
    C: ConnectionLike,
{
    let mut cmd = command(if options.read_only() {
        "XREAD"
    } else {
        "XREADGROUP"
    });
    cmd.arg(options).arg("STREAMS").arg(keys).arg(ids);

    let value = con.req_packed_command(&cmd).await?;

    StreamReadReply::from_redis_value(value)
}

// bulk(bulk(string-data('"my_stream_1"'), bulk(bulk(string-data('"1678280595282-0"'), bulk(string-data('"msg"'), string-data('"hi 0"'), field, value, ...)), ...)))
// LOL such nesting. This is still undesirable, as there are 5 layers of nested Vec. But at least we don't have to copy the bytes again.
impl StreamReadReply {
    /// Like [`redis::FromRedisValue`], but taking ownership instead of copying.
    fn from_redis_value(value: Value) -> RedisResult<Self> {
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
                            let mut values = values.into_iter();
                            let value_0 = values.next().unwrap();
                            let value_1 = values.next().unwrap();
                            let stream_key = string_from_redis_value(value_0)?;
                            let stream_key = StreamKey::new(stream_key).map_err(|_| {
                                let err: RedisError =
                                    (ErrorKind::ResponseError, "Invalid StreamKey").into();
                                err
                            })?;
                            match value_1 {
                                Value::Bulk(values) => {
                                    for value in values {
                                        match value {
                                            Value::Bulk(values) => {
                                                if values.len() != 2 {
                                                    return Err(err());
                                                }
                                                let mut values = values.into_iter();
                                                let value_0 = values.next().unwrap();
                                                let value_1 = values.next().unwrap();
                                                let id = string_from_redis_value(value_0)?;
                                                let (timestamp, sequence) =
                                                    parse_message_id(&id).map_err(|_| err())?;
                                                match value_1 {
                                                    Value::Bulk(values) => {
                                                        assert!(values.len() % 2 == 0);
                                                        let pairs = values.len() / 2;
                                                        let mut values = values.into_iter();
                                                        for _ in 0..pairs {
                                                            let field = values.next().unwrap();
                                                            let field =
                                                                string_from_redis_value(field)?;
                                                            let value = values.next().unwrap();
                                                            if &field == MSG {
                                                                let bytes =
                                                                    bytes_from_redis_value(value)?;
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

fn string_from_redis_value(v: Value) -> RedisResult<String> {
    match v {
        Value::Data(bytes) => Ok(String::from_utf8(bytes)?),
        Value::Okay => Ok("OK".to_owned()),
        Value::Status(val) => Ok(val),
        _ => Err((ErrorKind::TypeError, "Value not String").into()),
    }
}

fn bytes_from_redis_value(v: Value) -> RedisResult<Vec<u8>> {
    match v {
        Value::Data(bytes) => Ok(bytes),
        _ => Err((ErrorKind::TypeError, "Value not Data").into()),
    }
}
