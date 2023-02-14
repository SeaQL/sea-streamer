use nom::{
    bytes::complete::{is_not, take_while_m_n},
    character::complete::char,
    sequence::delimited,
    IResult,
};
use sea_streamer::{
    is_valid_stream_key_char, SequenceNo, ShardId, StreamKey, Timestamp, MAX_STREAM_KEY_LEN,
};
use thiserror::Error;
use time::{format_description::FormatItem, macros::format_description, PrimitiveDateTime};

pub const TIME_FORMAT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
// have no idea to how to make subsecond optional
pub const TIME_FORMAT_SUBSEC: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PartialMeta {
    pub timestamp: Option<Timestamp>,
    pub stream_key: Option<StreamKey>,
    pub sequence: Option<SequenceNo>,
    pub shard_id: Option<ShardId>,
}

#[derive(Error, Debug)]
pub enum ParseErr {
    #[error("Empty PartialMeta")]
    Empty,
    #[error("Unknown part: {0}")]
    Unknown(String),
}

pub fn parse_meta(input: &str) -> Result<(PartialMeta, &str), ParseErr> {
    let (o, raw) = match parens(input) {
        Ok(ok) => ok,
        Err(_) => {
            return Ok((
                PartialMeta {
                    timestamp: Some(Timestamp::now_utc()),
                    ..Default::default()
                },
                input,
            ))
        }
    };
    let parts = raw.split('|').map(|s| s.trim());
    let mut meta = PartialMeta::default();
    for part in parts {
        let mut parsed = false;
        if meta.timestamp.is_none()
            && meta.stream_key.is_none()
            && meta.sequence.is_none()
            && meta.shard_id.is_none()
        {
            if let Ok(timestamp) = parse_timestamp(part) {
                meta.timestamp = Some(timestamp.assume_utc());
                parsed = true;
            }
        }
        if !parsed && meta.stream_key.is_none() {
            if let Ok(("", stream_key)) = parse_stream_key(part) {
                meta.stream_key = Some(StreamKey::new(stream_key.to_owned()));
                parsed = true;
            }
        }
        if !parsed
            && meta.stream_key.is_some()
            && meta.sequence.is_none()
            && meta.shard_id.is_none()
        {
            if let Ok(sequence) = part.parse() {
                meta.sequence = Some(sequence);
                parsed = true;
            }
        }
        if !parsed
            && meta.stream_key.is_some()
            && meta.sequence.is_some()
            && meta.shard_id.is_none()
        {
            if let Ok(shard_id) = part.parse() {
                meta.shard_id = Some(ShardId::new(shard_id));
                parsed = true;
            }
        }
        if !parsed {
            return Err(ParseErr::Unknown(part.to_string()));
        }
    }
    if meta.timestamp.is_none()
        && meta.stream_key.is_none()
        && meta.sequence.is_none()
        && meta.shard_id.is_none()
    {
        return Err(ParseErr::Empty);
    }
    Ok((meta, o.trim()))
}

fn parse_timestamp(input: &str) -> Result<PrimitiveDateTime, time::error::Parse> {
    PrimitiveDateTime::parse(input, &TIME_FORMAT_SUBSEC)
        .or_else(|_| PrimitiveDateTime::parse(input, &TIME_FORMAT))
}

fn parse_stream_key(input: &str) -> IResult<&str, &str> {
    take_while_m_n(1, MAX_STREAM_KEY_LEN, is_valid_stream_key_char)(input)
}

fn parens(input: &str) -> IResult<&str, &str> {
    delimited(char('['), is_not("]"), char(']'))(input)
}

#[cfg(test)]
mod test {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn test_parse_meta_0() {
        assert_eq!(
            parse_meta(r#"a plain, raw message"#).unwrap().1,
            r#"a plain, raw message"#
        );
        assert_eq!(
            parse_meta(r#"{ "payload": "anything" }"#).unwrap().1,
            r#"{ "payload": "anything" }"#
        );
    }

    #[test]
    fn test_parse_meta_1() {
        assert_eq!(
            parse_meta(r#"[2022-01-02T03:04:05] { "payload": "anything" }"#).unwrap(),
            (
                PartialMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05).assume_utc()),
                    ..Default::default()
                },
                r#"{ "payload": "anything" }"#
            )
        );
    }

    #[test]
    fn test_parse_meta_2() {
        assert_eq!(
            parse_meta(r#"[2022-01-02T03:04:05.678 | my-fancy_topic.1] ["array", "of", "values"]"#)
                .unwrap(),
            (
                PartialMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05.678).assume_utc()),
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: None,
                    shard_id: None,
                },
                r#"["array", "of", "values"]"#
            )
        );
    }

    #[test]
    fn test_parse_meta_3() {
        assert_eq!(
            parse_meta(
                r#"[2022-01-02T03:04:05 | my-fancy_topic.1 | 123] { "payload": "anything" }"#
            )
            .unwrap(),
            (
                PartialMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05).assume_utc()),
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: Some(123),
                    shard_id: None,
                },
                r#"{ "payload": "anything" }"#
            )
        );
    }

    #[test]
    fn test_parse_meta_4() {
        assert_eq!(
            parse_meta(
                r#"[2022-01-02T03:04:05 | my-fancy_topic.1 | 123 | 4] { "payload": "anything" }"#
            )
            .unwrap(),
            (
                PartialMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05).assume_utc()),
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: Some(123),
                    shard_id: Some(ShardId::new(4)),
                },
                r#"{ "payload": "anything" }"#
            )
        );
    }

    #[test]
    fn test_parse_meta_5() {
        assert_eq!(
            parse_meta(r#"[my-fancy_topic.1] { "payload": "anything" }"#).unwrap(),
            (
                PartialMeta {
                    timestamp: None,
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: None,
                    shard_id: None,
                },
                r#"{ "payload": "anything" }"#
            )
        );
    }

    #[test]
    fn test_parse_meta_6() {
        assert_eq!(
            parse_meta(r#"[my-fancy_topic.1 | 123] ["array", "of", "values"]"#).unwrap(),
            (
                PartialMeta {
                    timestamp: None,
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: Some(123),
                    shard_id: None,
                },
                r#"["array", "of", "values"]"#
            )
        );
    }

    #[test]
    fn test_parse_meta_7() {
        assert_eq!(
            parse_meta(r#"[my-fancy_topic.1 | 123 | 4] { "payload": "anything" }"#).unwrap(),
            (
                PartialMeta {
                    timestamp: None,
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: Some(123),
                    shard_id: Some(ShardId::new(4)),
                },
                r#"{ "payload": "anything" }"#
            )
        );
    }

    #[test]
    fn test_parse_meta_error_1() {
        assert!(matches!(parse_meta(r#"[ ]"#), Err(ParseErr::Unknown(_))))
    }
}
