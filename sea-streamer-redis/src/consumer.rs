use std::{thread::ThreadId, time::Duration};

use flume::{
    bounded,
    r#async::{RecvFut, RecvStream},
    Receiver, RecvError,
};
use redis::{
    aio::ConnectionLike, cmd as command, streams::StreamReadOptions, AsyncCommands, ErrorKind,
};
use sea_streamer_runtime::{sleep, spawn_task};

use crate::{host_id, map_err, RedisCluster, RedisErr, RedisResult};
use sea_streamer_types::{
    export::{
        async_trait,
        futures::{future::Map, FutureExt},
        url::Url,
    },
    Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions, Message, SeqPos, ShardId,
    SharedMessage, StreamErr, StreamKey, Timestamp,
};

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StreamReadReply(pub(crate) Vec<SharedMessage>);

#[derive(Debug)]
pub struct RedisConsumer {
    receiver: Receiver<RedisResult<SharedMessage>>,
}

#[derive(Debug, Clone)]
pub struct RedisConsumerOptions {
    mode: ConsumerMode,
    group: Option<ConsumerGroup>,
    shared_shard: bool,
    consumer_timeout: Option<Duration>,
    auto_stream_reset: AutoStreamReset,
    enable_auto_commit: bool,
    auto_commit_interval: Duration,
}

pub const DEFAULT_AUTO_COMMIT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
pub const DOLLAR: &str = "$";
pub const BATCH_SIZE: usize = 100;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AutoStreamReset {
    /// Use `0` as ID, which is the earliest message.
    Earliest,
    /// Use `$` as ID, which is the latest message.
    Latest,
}

// async fn xread_options<C, K, ID>(
//     con: &mut C,
//     keys: &[K],
//     ids: &[ID],
//     options: &StreamReadOptions,
// ) -> RedisResult<StreamReadReply>
// where
//     K: ToRedisArgs,
//     ID: ToRedisArgs,
//     C: ConnectionLike,
// {
//     let mut cmd = command(if options.read_only() {
//         "XREAD"
//     } else {
//         "XREADGROUP"
//     });
//     cmd.arg(options).arg("STREAMS").arg(keys).arg(ids);

//     let value = con.req_packed_command(&cmd).await.map_err(map_err)?;

//     StreamReadReply::from_redis_value(value)
// }

pub type NextFuture<'a> = Map<
    RecvFut<'a, RedisResult<SharedMessage>>,
    fn(Result<RedisResult<SharedMessage>, RecvError>) -> RedisResult<SharedMessage>,
>;

impl Default for RedisConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

impl ConsumerOptions for RedisConsumerOptions {
    type Error = RedisErr;

    fn new(mode: ConsumerMode) -> Self {
        Self {
            mode,
            group: None,
            shared_shard: true,
            consumer_timeout: None,
            auto_stream_reset: AutoStreamReset::Latest,
            enable_auto_commit: true,
            auto_commit_interval: DEFAULT_AUTO_COMMIT_INTERVAL,
        }
    }

    fn mode(&self) -> RedisResult<&ConsumerMode> {
        Ok(&self.mode)
    }

    /// ### Consumer ID
    ///
    /// Unlike Kafka, Redis requires consumers to self-assign consumer IDs.
    /// SeaStreamer uses a combination of `host id` + `process id` + `thread id` + `sub-second`.
    fn consumer_group(&self) -> RedisResult<&ConsumerGroup> {
        self.group.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    /// SeaStreamer Redis offers two load-balancing mechanisms:
    ///
    /// ### (Fine-grained) Shared shard
    ///
    /// Multiple consumers in the same group can share the same shard.
    /// This is load-balanced in a first-ask-first-served manner, according to the Redis documentation.
    /// This can be considered dynamic load-balancing: faster consumers will consume more messages.
    ///
    /// ### (Coarse) Owned shard
    ///
    /// Multiple consumers within the same group do not share a shard.
    /// Each consumer will attempt to claim ownership of a shard, and other consumers will not step in.
    /// However, if a consumer has been idle for too long (defined by `consumer_timeout`),
    /// another consumer will step in and kick the other consumer out of the group.
    ///
    /// This mimicks Kafka's consumer group behaviour.
    ///
    /// This is reconciled among consumers via a probabilistic contention avoidance mechanism,
    /// which should be fine with < 100 consumers in the same group.
    fn set_consumer_group(&mut self, group: ConsumerGroup) -> RedisResult<&mut Self> {
        self.group = Some(group);
        Ok(self)
    }
}

impl RedisConsumerOptions {
    /// Default is true.
    pub fn shared_shard(&self) -> bool {
        self.shared_shard
    }
    pub fn set_shared_shard(&mut self, shared_shard: bool) -> &mut Self {
        self.shared_shard = shared_shard;
        self
    }

    /// If None, defaults to [`crate::DEFAULT_TIMEOUT`].
    pub fn consumer_timeout(&self) -> Option<&Duration> {
        self.consumer_timeout.as_ref()
    }
    pub fn set_consumer_timeout(&mut self, consumer_timeout: Option<Duration>) -> &mut Self {
        self.consumer_timeout = consumer_timeout;
        self
    }

    /// Where to stream from when the consumer group does not exists.
    ///
    /// If unset, defaults to Latest.
    pub fn set_auto_stream_reset(&mut self, v: AutoStreamReset) -> &mut Self {
        self.auto_stream_reset = v;
        self
    }
    pub fn auto_stream_reset(&self) -> &AutoStreamReset {
        &self.auto_stream_reset
    }

    /// If enabled, read with `NOACK`. This acknowledges messages as soon as they are read.
    /// If you want to commit only what have been explicitly acked, set it to false.
    ///
    /// If unset, defaults to true.
    pub fn set_enable_auto_commit(&mut self, v: bool) -> &mut Self {
        self.enable_auto_commit = v;
        self
    }
    pub fn enable_auto_commit(&self) -> &bool {
        &self.enable_auto_commit
    }

    /// The interval for acks to be committed to the server.
    /// This option is only relevant when `enable_auto_commit` is false.
    ///
    /// If unset, defaults to [`DEFAULT_AUTO_COMMIT_INTERVAL`].
    pub fn set_auto_commit_interval(&mut self, v: Duration) -> &mut Self {
        self.auto_commit_interval = v;
        self
    }
    pub fn auto_commit_interval(&self) -> &Duration {
        &self.auto_commit_interval
    }
}

#[async_trait]
impl Consumer for RedisConsumer {
    type Error = RedisErr;
    type Message<'a> = SharedMessage;
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = RecvStream<'a, RedisResult<SharedMessage>>;

    async fn seek(&mut self, _: Timestamp) -> RedisResult<()> {
        todo!()
    }

    fn rewind(&mut self, _: SeqPos) -> RedisResult<()> {
        todo!()
    }

    fn assign(&mut self, _: ShardId) -> RedisResult<()> {
        todo!()
    }

    fn next(&self) -> Self::NextFuture<'_> {
        self.receiver.recv_async().map(|res| match res {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(StreamErr::Backend(RedisErr::ClientError(
                "Consumer died with unrecoverable error. Check the log for details.".to_owned(),
            ))),
        })
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        todo!()
    }
}

/// The caller should supply the thread id, which should be where `create_consumer` is called.
pub fn consumer_id_for(thread_id: ThreadId) -> String {
    let thread_id = format!("{:?}", thread_id);
    format!(
        "{}:{}:{}:{}",
        host_id(),
        std::process::id(),
        thread_id
            .trim_start_matches("ThreadId(")
            .trim_end_matches(')'),
        Timestamp::now_utc().millisecond()
    )
}

struct ShardedStream {
    shards: Vec<Shard>,
}

struct Shard {
    key: String,
    id: Option<(u64, u16)>,
}

impl Shard {
    fn update(&mut self, msg: &SharedMessage) {
        self.id = Some((
            (msg.timestamp().unix_timestamp_nanos() / 1_000_000)
                .try_into()
                .expect("RedisConsumer: timestamp out of range"),
            (msg.sequence() & 0xFFFF).try_into().expect("Never fails"),
        ));
    }
}

pub(crate) async fn create_consumer(
    mut cluster: RedisCluster,
    options: RedisConsumerOptions,
    stream_keys: Vec<StreamKey>,
) -> RedisResult<RedisConsumer> {
    cluster.reconnect_all().await?; // init connections
    let (sender, receiver) = bounded(1);
    let mut streams = Vec::new();
    for key in stream_keys {
        let conn = cluster.get_any()?;
        streams.push(discover_shards(conn, key).await?);
    }
    let opts = StreamReadOptions::default().count(BATCH_SIZE).block(0);

    let protocol = cluster.protocol().unwrap().to_owned();
    let moved_to = move |err: redis::RedisError| -> Url {
        match err.redirect_node() {
            Some((to, _slot)) => {
                // `to` must be in form of `host:port` without protocol
                format!("{}://{}", protocol, to)
                    .parse()
                    .expect("Failed to parse URL: {to}")
            }
            None => panic!("Key is moved, but to where? {err:?}"),
        }
    };

    if options.mode == ConsumerMode::RealTime {
        // This is the simple path
        spawn_task(async move {
            loop {
                // TODO optimize by reading multiple streams in one XREAD
                for stream in streams.iter_mut() {
                    for stream_shard in stream.shards.iter_mut() {
                        let (node, conn) = match cluster.get_connection_for(&stream_shard.key).await
                        {
                            Ok(conn) => conn,
                            Err(StreamErr::Backend(RedisErr::TryAgain(_))) => continue, // it will sleep inside `get_connection`
                            Err(err) => {
                                sender.send_async(Err(err)).await.ok();
                                return;
                            }
                        };

                        let mut cmd = command("XREAD");
                        cmd.arg(&opts).arg("STREAMS").arg(stream_shard.key.as_str());
                        if let Some((a, b)) = &stream_shard.id {
                            cmd.arg(format!("{a}-{b}"));
                        } else {
                            cmd.arg(DOLLAR);
                        }

                        match conn.req_packed_command(&cmd).await {
                            Ok(value) => match StreamReadReply::from_redis_value(value) {
                                Ok(res) => {
                                    if let Some(msg) = res.0.last() {
                                        stream_shard.update(msg);
                                    }
                                    for msg in res.0 {
                                        sender.send_async(Ok(msg)).await.ok();
                                    }
                                }
                                Err(err) => {
                                    sender.send_async(Err(err)).await.ok();
                                    return;
                                }
                            },
                            Err(err) => {
                                let kind = err.kind();
                                if kind == ErrorKind::Moved {
                                    cluster.moved(stream_shard.key.as_str(), moved_to(err));
                                } else if kind == ErrorKind::IoError {
                                    let node = node.to_owned();
                                    cluster.reconnect(&node).ok();
                                } else if matches!(
                                    kind,
                                    ErrorKind::ClusterDown | ErrorKind::MasterDown
                                ) {
                                    sleep(Duration::from_secs(1)).await;
                                } else {
                                    if matches!(kind, ErrorKind::Ask | ErrorKind::TryAgain) {
                                        panic!("Impossible cluster error: {err}");
                                    }
                                    // unrecoverable
                                    sender.send_async(Err(map_err(err))).await.ok();
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        });
    } else {
        todo!()
    }

    Ok(RedisConsumer { receiver })
}

async fn discover_shards(
    conn: &mut redis::aio::Connection,
    stream: StreamKey,
) -> RedisResult<ShardedStream> {
    let shards: Vec<String> = conn
        .keys(format!("{}:*", stream.name()))
        .await
        .map_err(map_err)?;
    let shards = if shards.is_empty() {
        vec![Shard {
            key: stream.name().to_owned(),
            id: None,
        }]
    } else {
        shards
            .into_iter()
            .filter_map(|key| match key.split_once(':') {
                Some((_, tail)) => {
                    // make sure we can parse the tail
                    if tail.parse::<u64>().is_ok() {
                        Some(Shard { key, id: None })
                    } else {
                        log::warn!("Ignoring `{key}`");
                        None
                    }
                }
                None => unreachable!(),
            })
            .collect()
    };

    Ok(ShardedStream { shards })
}
