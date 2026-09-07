use crate::{
    MessageField, RedisCluster, RedisErr, RedisMessage, RedisResult, StreamRangeReply,
    TimestampFormat, int_from_redis_value, map_err, string_from_redis_value,
};
use redis::{Value, aio::ConnectionLike, cmd as command};
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

/// How `XTRIM` picks the entries to remove.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum TrimMode {
    /// `~`: trim whole radix-tree nodes, may leave a few more entries than asked for.
    /// Cheap for the server, so the default.
    #[default]
    Approx,
    /// `=`: trim exactly to the threshold. Use when the precise result matters,
    /// e.g. `MAXLEN 1` to empty a stream while keeping its last entry.
    Exact,
}

impl TrimMode {
    fn arg(&self) -> &'static str {
        match self {
            Self::Approx => "~",
            Self::Exact => "=",
        }
    }
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

    /// XRANGE
    ///
    /// Ref: https://redis.io/docs/latest/commands/xrange/
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
            cmd.arg("COUNT");
            cmd.arg(count);
        }

        log::debug!(
            "XRANGE: {} {} {}",
            key.name(),
            start.format(ts_fmt),
            end.format(ts_fmt)
        );
        match conn.req_packed_command(&cmd).await {
            Ok(value) => {
                let messages = StreamRangeReply::from_redis_value(value, key, ts_fmt, msg)?.0;
                log::debug!("Range got {} messages", messages.len());
                Ok(messages)
            }
            Err(err) => Err(map_err(err)),
        }
    }

    /// `XLEN`: number of entries in the stream. A missing key counts as 0.
    ///
    /// Ref: https://redis.io/docs/latest/commands/xlen/
    pub async fn xlen(&mut self, key: &StreamKey) -> RedisResult<u64> {
        let conn = self.cluster.get_connection_for(key.name()).await?.1;

        let mut cmd = command("XLEN");
        cmd.arg(key.name());

        log::debug!("XLEN: {}", key.name());
        match conn.req_packed_command(&cmd).await {
            Ok(value) => Ok(int_from_redis_value(value)?.try_into().map_err(err)?),
            Err(err) => Err(map_err(err)),
        }
    }

    /// `XTRIM <key> MAXLEN ~|= <max_len>`, returning the number of entries removed.
    ///
    /// Ref: https://redis.io/docs/latest/commands/xtrim/
    pub async fn trim_max_len(
        &mut self,
        key: &StreamKey,
        max_len: u64,
        mode: TrimMode,
    ) -> RedisResult<u64> {
        let mut cmd = command("XTRIM");
        cmd.arg(key.name())
            .arg("MAXLEN")
            .arg(mode.arg())
            .arg(max_len);

        log::debug!("XTRIM: {} MAXLEN {} {}", key.name(), mode.arg(), max_len);
        self.xtrim(key, cmd).await
    }

    /// `XTRIM <key> MINID ~|= <timestamp>`, returning the number of entries removed.
    /// Every entry with an ID below `timestamp` goes; the timestamp is rendered in the
    /// streamer's [`TimestampFormat`], the same way stream IDs are read back.
    ///
    /// Ref: https://redis.io/docs/latest/commands/xtrim/
    pub async fn trim_min_id(
        &mut self,
        key: &StreamKey,
        timestamp: Timestamp,
        mode: TrimMode,
    ) -> RedisResult<u64> {
        let min_id = IdRange::Ts(timestamp).format(self.options.timestamp_format);

        let mut cmd = command("XTRIM");
        cmd.arg(key.name())
            .arg("MINID")
            .arg(mode.arg())
            .arg(&min_id);

        log::debug!("XTRIM: {} MINID {} {}", key.name(), mode.arg(), min_id);
        self.xtrim(key, cmd).await
    }

    async fn xtrim(&mut self, key: &StreamKey, cmd: redis::Cmd) -> RedisResult<u64> {
        let conn = self.cluster.get_connection_for(key.name()).await?.1;

        match conn.req_packed_command(&cmd).await {
            Ok(value) => Ok(int_from_redis_value(value)?.try_into().map_err(err)?),
            Err(err) => Err(map_err(err)),
        }
    }
}

// bulk(string-data('"0"'), bulk(string-data('"stream-0"')))
impl ScanResult {
    pub(crate) fn from_redis_value(value: Value) -> RedisResult<Self> {
        let mut cursor = String::new();
        let mut streams = Vec::new();

        if let Value::Array(values) = value {
            if values.len() != 2 {
                return Err(err(values));
            }
            let mut values = values.into_iter();
            let value_0 = values.next().unwrap();
            let value_1 = values.next().unwrap();

            cursor = string_from_redis_value(value_0)?;

            if let Value::Array(values) = value_1 {
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
