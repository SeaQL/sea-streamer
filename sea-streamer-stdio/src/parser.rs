//! The SeaStreamer file format should be simple (like ndjson):
//!
//! ```json
//! [timestamp | stream key | sequence] { "payload": "anything" }
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
//! [my_topic] "A string payload"
//! [my_topic | 123] { "payload": "anything" }
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
use sea_streamer::{export::DateTime, SequenceNo, StreamKey};
use serde::de::DeserializeOwned;
pub use serde_json::Value as Json;
use thiserror::Error;
use time::macros::format_description;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MessageMeta {
    timestamp: Option<DateTime>,
    stream_key: Option<StreamKey>,
    sequence: Option<SequenceNo>,
}

#[derive(Error, Debug)]
pub enum ParseErr {
    #[error("Empty MessageMeta")]
    Empty,
    #[error("Error parsing parens: {0}")]
    Nom(String),
    #[error("Error parsing JSON: {0}")]
    Json(String),
    #[error("Unknown part: {0}")]
    Unknown(String),
}

pub fn parse_msg_json<V: DeserializeOwned>(input: &str) -> Result<(MessageMeta, V), ParseErr> {
    let (meta, payload) = parse_meta(input)?;
    let json = serde_json::from_str(payload).map_err(|e| ParseErr::Json(e.to_string()))?;
    Ok((meta, json))
}

pub fn parse_meta(input: &str) -> Result<(MessageMeta, &str), ParseErr> {
    let (o, raw) = parens(input).map_err(|e| ParseErr::Nom(e.to_string()))?;
    let parts = raw.split('|').map(|s| s.trim());
    let mut meta = MessageMeta::default();
    for part in parts {
        let mut parsed = false;
        if meta.timestamp.is_none() && meta.stream_key.is_none() && meta.sequence.is_none() {
            let format = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
            if let Ok(timestamp) = DateTime::parse(part, &format) {
                meta.timestamp = Some(timestamp);
                parsed = true;
            }
        }
        if meta.stream_key.is_none() && meta.sequence.is_none() {
            if let Ok(("", stream_key)) = parse_stream_key(part) {
                meta.stream_key = Some(StreamKey::new(stream_key.to_owned()));
                parsed = true;
            }
        }
        if meta.sequence.is_none() {
            if let Ok(sequence) = part.parse() {
                meta.sequence = Some(sequence);
                parsed = true;
            }
        }
        if !parsed {
            return Err(ParseErr::Unknown(part.to_string()));
        }
    }
    if meta.timestamp.is_none() && meta.stream_key.is_none() && meta.sequence.is_none() {
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
                MessageMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05)),
                    ..Default::default()
                },
                r#"{ "payload": "anything" }"#
            )
        )
    }

    #[test]
    fn test_parse_meta_2() {
        assert_eq!(
            parse_meta(r#"[2022-01-02T03:04:05 | my-fancy_topic.1] { "payload": "anything" }"#)
                .unwrap(),
            (
                MessageMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05)),
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: None,
                },
                r#"{ "payload": "anything" }"#
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
                MessageMeta {
                    timestamp: Some(datetime!(2022-01-02 03:04:05)),
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: Some(123),
                },
                r#"{ "payload": "anything" }"#
            )
        )
    }

    #[test]
    fn test_parse_meta_4() {
        assert_eq!(
            parse_meta(r#"[my-fancy_topic.1] { "payload": "anything" }"#).unwrap(),
            (
                MessageMeta {
                    timestamp: None,
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: None,
                },
                r#"{ "payload": "anything" }"#
            )
        )
    }

    #[test]
    fn test_parse_meta_5() {
        assert_eq!(
            parse_meta(r#"[my-fancy_topic.1 | 123] { "payload": "anything" }"#).unwrap(),
            (
                MessageMeta {
                    timestamp: None,
                    stream_key: Some(StreamKey::new("my-fancy_topic.1".to_owned())),
                    sequence: Some(123),
                },
                r#"{ "payload": "anything" }"#
            )
        )
    }

    #[test]
    fn test_parse_meta_error_1() {
        assert!(matches!(parse_meta(r#"[ ]"#), Err(ParseErr::Unknown(_))))
    }

    #[test]
    fn test_parse_1() {
        assert_eq!(
            parse_msg_json(r#"[my_topic] { "payload": "anything" }"#).unwrap(),
            (
                MessageMeta {
                    timestamp: None,
                    stream_key: Some(StreamKey::new("my_topic".to_owned())),
                    sequence: None,
                },
                serde_json::from_str::<Json>(r#"{ "payload": "anything" }"#).unwrap()
            )
        )
    }
}
