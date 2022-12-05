//! The SeaStreamer file format should be simple (like ndjson):
//!
//! ```json
//! [timestamp | stream key | sequence | shard_id] { "payload": "anything" }
//! ```
//!
//! The square brackets are literal `[]`.
//!
//! The following are all valid:
//!
//! ```json
//! [2022-01-01T00:00:00] { "payload": "anything" }
//! [2022-01-01T00:00:00 | my_topic] "A string payload"
//! [2022-01-01T00:00:00 | my-topic-2 | 123] ["array", "of", "values"]
//! [2022-01-01T00:00:00 | my-topic-2 | 123 | 4] { "payload": "anything" }
//! [my_topic] "A string payload"
//! [my_topic | 123] { "payload": "anything" }
//! [my_topic | 123 | 4] { "payload": "anything" }
//! ```
//!
//! The following are all invalid:
//!
//! ```json
//! [Jan 1, 2022] { "payload": "anything" }
//! [2022-01-01T00:00:00] 12345
//! ```

use nom::{
    bytes::complete::{is_not, take_while_m_n},
    character::complete::char,
    sequence::delimited,
    IResult,
};
use sea_streamer::{SequenceNo, ShardId, StreamKey, Timestamp};
pub use serde_json::Value as Json;
use thiserror::Error;
use time::{format_description::FormatItem, macros::format_description, PrimitiveDateTime};

pub const TIME_FORMAT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]");

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
    #[error("Error parsing parens: {0}")]
    Nom(String),
    #[error("Unknown part: {0}")]
    Unknown(String),
}

pub fn parse_meta(input: &str) -> Result<(PartialMeta, &str), ParseErr> {
    let (o, raw) = parens(input).map_err(|e| ParseErr::Nom(e.to_string()))?;
    let parts = raw.split('|').map(|s| s.trim());
    let mut meta = PartialMeta::default();
    for part in parts {
        let mut parsed = false;
        if meta.timestamp.is_none()
            && meta.stream_key.is_none()
            && meta.sequence.is_none()
            && meta.shard_id.is_none()
        {
            if let Ok(timestamp) = PrimitiveDateTime::parse(part, &TIME_FORMAT) {
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

fn is_valid_stream_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}

fn parse_stream_key(input: &str) -> IResult<&str, &str> {
    // https://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations
    take_while_m_n(1, 249, is_valid_stream_key_char)(input)
}

fn parens(input: &str) -> IResult<&str, &str> {
    delimited(char('['), is_not("]"), char(']'))(input)
}

#[cfg(test)]
mod test {
    use time::macros::datetime;

    use super::*;

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
        )
    }

    #[test]
    fn test_parse_meta_2() {
        assert_eq!(
            parse_meta(r#"[2022-01-02T03:04:05 | my-fancy_topic.1] ["array", "of", "values"]"#)
                .unwrap(),
            (
                PartialMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05).assume_utc()),
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: None,
                    shard_id: None,
                },
                r#"["array", "of", "values"]"#
            )
        )
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
        )
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
        )
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
        )
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
        )
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
        )
    }

    #[test]
    fn test_parse_meta_error_1() {
        assert!(matches!(parse_meta(r#"[ ]"#), Err(ParseErr::Unknown(_))))
    }
}
