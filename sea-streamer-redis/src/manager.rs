use crate::{
    map_err, string_from_redis_value, MessageField, RedisCluster, RedisErr, RedisMessage,
    RedisResult, StreamRangeReply, TimestampFormat,
};
use redis::{aio::ConnectionLike, cmd as command, Value};
use sea_streamer_types::{StreamErr, StreamKey, Timestamp};

#[derive(Debug)]
pub struct RedisManager {
    cluster: RedisCluster,
    options: RedisManagerOptions,
}

#[derive(Debug, Default, Clone)]
/// Options for Manager
pub struct RedisManagerOptions {
    pub(crate) timestamp_format: TimestampFormat,
    pub(crate) message_field: MessageField,
}

#[derive(Debug)]
pub struct ScanResult {
    pub cursor: String,
    pub streams: Vec<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IdRange {
    /// Inclusive
    Ts(Timestamp),
    /// Exclusive
    TsEx(Timestamp),
    /// -
    Minus,
    /// +
    Plus,
}

pub(crate) async fn create_manager(
    mut cluster: RedisCluster,
    options: RedisManagerOptions,
) -> RedisResult<RedisManager> {
    cluster.reconnect_all().await?; // init connections

    Ok(RedisManager { cluster, options })
}

impl RedisManager {
    pub async fn scan(&mut self, cursor: &str) -> RedisResult<ScanResult> {
        let conn = self.cluster.get_connection_for("").await?.1;

        let mut cmd = command("SCAN");
        cmd.arg(if cursor.is_empty() { "0" } else { cursor })
            .arg("TYPE")
            .arg("stream");

        log::debug!("SCAN");
        match conn.req_packed_command(&cmd).await {
            Ok(value) => {
                log::debug!("Scan result {value:?}");
                Ok(ScanResult::from_redis_value(value)?)
            }
            Err(err) => Err(map_err(err)),
        }
    }

    pub async fn range(
        &mut self,
        key: StreamKey,
        start: IdRange,
        end: IdRange,
        count: Option<usize>,
    ) -> RedisResult<Vec<RedisMessage>> {
        let conn = self.cluster.get_connection_for("").await?.1;

        let ts_fmt = self.options.timestamp_format;
        let msg = self.options.message_field;

        let mut cmd = command("XRANGE");
        cmd.arg(key.name())
            .arg(start.format(ts_fmt))
            .arg(end.format(ts_fmt));
        if let Some(count) = count {
            cmd.arg(count);
        }

        log::debug!("XRANGE");
        match conn.req_packed_command(&cmd).await {
            Ok(value) => {
                let messages = StreamRangeReply::from_redis_value(value, key, ts_fmt, msg)?.0;
                log::debug!("Range got {} messages", messages.len());
                Ok(messages)
            }
            Err(err) => Err(map_err(err)),
        }
    }
}

// bulk(string-data('"0"'), bulk(string-data('"stream-0"')))
impl ScanResult {
    pub(crate) fn from_redis_value(value: Value) -> RedisResult<Self> {
        let mut cursor = String::new();
        let mut streams = Vec::new();

        if let Value::Bulk(values) = value {
            if values.len() != 2 {
                return Err(err(values));
            }
            let mut values = values.into_iter();
            let value_0 = values.next().unwrap();
            let value_1 = values.next().unwrap();

            cursor = string_from_redis_value(value_0)?;

            if let Value::Bulk(values) = value_1 {
                for value in values {
                    streams.push(string_from_redis_value(value)?);
                }
            }
        }

        Ok(Self { cursor, streams })
    }
}

impl IdRange {
    pub fn format(&self, timestamp_format: TimestampFormat) -> String {
        match self {
            Self::Ts(ts) => match timestamp_format {
                TimestampFormat::UnixTimestampMillis => {
                    format!("{}", ts.unix_timestamp_nanos() / 1_000_000)
                }
                #[cfg(feature = "nanosecond-timestamp")]
                TimestampFormat::UnixTimestampNanos => format!("{}", ts.unix_timestamp_nanos()),
            },
            Self::TsEx(ts) => match timestamp_format {
                TimestampFormat::UnixTimestampMillis => {
                    format!("({}", ts.unix_timestamp_nanos() / 1_000_000)
                }
                #[cfg(feature = "nanosecond-timestamp")]
                TimestampFormat::UnixTimestampNanos => format!("({}", ts.unix_timestamp_nanos()),
            },
            Self::Minus => "-".to_string(),
            Self::Plus => "+".to_string(),
        }
    }
}

fn err<D: std::fmt::Debug>(d: D) -> StreamErr<RedisErr> {
    StreamErr::Backend(RedisErr::ResponseError(format!("{d:?}")))
}
