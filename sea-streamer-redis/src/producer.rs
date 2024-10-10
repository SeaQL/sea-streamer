use flume::{bounded, r#async::RecvFut, unbounded, Sender};
use redis::{aio::ConnectionLike, cmd as command, ErrorKind, Pipeline};
use std::{fmt::Debug, future::Future, sync::Arc, time::Duration};

use crate::{
    map_err, parse_message_id, string_from_redis_value, MessageField, RedisCluster, RedisErr,
    RedisResult, TimestampFormat, ZERO,
};
use sea_streamer_runtime::{sleep, spawn_task};
use sea_streamer_types::{
    export::futures::FutureExt, Buffer, MessageHeader, Producer, ProducerOptions, ShardId,
    StreamErr, StreamKey, Timestamp, SEA_STREAMER_INTERNAL,
};

const MAX_RETRY: usize = 100;

#[derive(Debug, Clone)]
/// The Redis Producer.
pub struct RedisProducer {
    stream: Option<StreamKey>,
    sender: Sender<SendRequest>,
}

#[derive(Default, Clone)]
/// Options for Producers, including sharding.
pub struct RedisProducerOptions {
    sharder: Option<Arc<dyn SharderConfig>>,
    pub(crate) timestamp_format: TimestampFormat,
    pub(crate) message_field: MessageField,
}

impl Debug for RedisProducerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisProducerOptions")
            .field("sharder", &self.sharder.as_ref())
            .finish()
    }
}

type Receipt = Sender<RedisResult<MessageHeader>>;

struct SendRequest {
    stream_key: StreamKey,
    bytes: Vec<u8>,
    receipt: Receipt,
}

/// A future that returns a Send Receipt. This future is cancel safe.
pub struct SendFuture {
    fut: RecvFut<'static, RedisResult<MessageHeader>>,
}

impl Debug for SendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFuture").finish()
    }
}

/// Trait to instantiate new sharders. It should also impl `Debug` so it can be named.
pub trait SharderConfig: Debug + Send + Sync {
    /// Each producer will create its own sharder.
    /// They should not have any shared state for the sake of concurrency.
    fn init(&self) -> Box<dyn Sharder>;
}

/// Trait that sharding strategies should implement. It should also impl `Debug` so its states can be inspected.
pub trait Sharder: Debug + Send {
    /// Return the determined shard id for the given message.
    /// This should be a *real quick* computation, otherwise this can become the bottleneck of streaming.
    /// Mutex, atomic or anything that can create contention will be disastrous.
    ///
    /// It will then be sent to the stream with key `STREAM_KEY:SHARD_ID`.
    /// The Redis Cluster will assign this shard to a particular node as the cluster scales.
    /// Different shards may or may not end up in the same slot, and thus may or may not end up in the same node.
    fn shard(&mut self, stream_key: &StreamKey, bytes: &[u8]) -> u64;
}

#[derive(Debug, Clone)]
/// Shard streams pseudo-randomly but fairly. Basically a `rand() % num_shards`.
pub struct PseudoRandomSharder {
    num_shards: u64,
}

#[derive(Debug, Clone)]
/// Shard streams by round-robin.
pub struct RoundRobinSharder {
    num_shards: u32,
    state: u32,
}

impl Producer for RedisProducer {
    type Error = RedisErr;
    type SendFuture = SendFuture;

    fn send_to<S: Buffer>(&self, stream: &StreamKey, payload: S) -> RedisResult<Self::SendFuture> {
        // one shot channel
        let (sender, receiver) = bounded(1);
        // unbounded, so never blocks
        self.sender
            .send(SendRequest {
                stream_key: stream.to_owned(),
                bytes: payload.into_bytes(),
                receipt: sender,
            })
            .map_err(|_| StreamErr::Backend(RedisErr::ProducerDied))?;

        Ok(SendFuture {
            fut: receiver.into_recv_async(),
        })
    }

    #[inline]
    async fn end(mut self) -> RedisResult<()> {
        self.flush().await
    }

    #[inline]
    async fn flush(&mut self) -> RedisResult<()> {
        // The trick here is to send a signal message and wait for the receipt.
        // By the time it returns a receipt, everything before should have already been sent.
        let null = [];
        self.send_to(&StreamKey::new(SEA_STREAMER_INTERNAL)?, null.as_slice())?
            .await?;
        Ok(())
    }

    fn anchor(&mut self, stream: StreamKey) -> RedisResult<()> {
        if self.stream.is_none() {
            self.stream = Some(stream);
            Ok(())
        } else {
            Err(StreamErr::AlreadyAnchored)
        }
    }

    fn anchored(&self) -> RedisResult<&StreamKey> {
        if let Some(stream) = &self.stream {
            Ok(stream)
        } else {
            Err(StreamErr::NotAnchored)
        }
    }
}

impl ProducerOptions for RedisProducerOptions {}

impl RedisProducerOptions {
    /// Assign a sharder.
    ///
    /// Sharding simply means splitting a stream into multiple keys.
    /// These keys can then be handled by different nodes in a cluster.
    /// Since shards (group of keys) can be moved across nodes on the fly,
    /// it is recommended to over-shard for better key distribution.
    pub fn set_sharder<S: SharderConfig + 'static>(&mut self, v: S) -> &mut Self {
        self.sharder = Some(Arc::new(v));
        self
    }
    /// Reset sharder to None.
    pub fn clear_sharder(&mut self) -> &mut Self {
        self.sharder = None;
        self
    }
    /// Get the currently assigned sharder.
    pub fn sharder(&self) -> Option<&dyn SharderConfig> {
        self.sharder.as_deref()
    }
}

impl Future for SendFuture {
    type Output = RedisResult<MessageHeader>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.fut.poll_unpin(cx) {
            std::task::Poll::Ready(res) => std::task::Poll::Ready(match res {
                Ok(res) => res,
                Err(_) => Err(StreamErr::Backend(RedisErr::ProducerDied)),
            }),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub(crate) async fn create_producer(
    mut cluster: RedisCluster,
    mut options: RedisProducerOptions,
) -> RedisResult<RedisProducer> {
    cluster.reconnect_all().await?; // init connections
    let (sender, receiver) = unbounded();
    let mut sharder = options.sharder.take().map(|a| a.init());
    let timestamp_format = options.timestamp_format;
    let message_field = options.message_field;

    // Redis commands are exclusive (`&mut self`), so we need a producer task
    spawn_task(async move {
        // exit if all senders have been dropped
        while let Ok(request) = receiver.recv_async().await {
            let mut requests = vec![request];
            requests.extend(receiver.drain());
            let mut remaining = requests.len();
            let mut requests = requests.into_iter();
            let mut batch: (Vec<(String, StreamKey, ShardId, Receipt)>, Pipeline) =
                Default::default();
            let mut next_batch = batch.clone();
            while remaining > 0 {
                for SendRequest {
                    stream_key,
                    bytes,
                    receipt,
                } in requests.by_ref()
                {
                    if stream_key.name() == SEA_STREAMER_INTERNAL && bytes.is_empty() {
                        // A signalling message
                        next_batch.0.push((
                            SEA_STREAMER_INTERNAL.to_owned(),
                            stream_key,
                            ZERO,
                            receipt,
                        ));
                        break;
                    } else {
                        let redis_stream_key;
                        let (redis_key, shard) = if let Some(sharder) = sharder.as_mut() {
                            let shard = sharder.shard(&stream_key, bytes.as_slice());
                            redis_stream_key = format!("{name}:{shard}", name = stream_key.name());
                            (redis_stream_key.as_str(), ShardId::new(shard))
                        } else {
                            (stream_key.name(), ZERO)
                        };
                        let mut cmd = command("XADD");
                        cmd.arg(redis_key);
                        match timestamp_format {
                            TimestampFormat::UnixTimestampMillis => cmd.arg("*"),
                            #[cfg(feature = "nanosecond-timestamp")]
                            TimestampFormat::UnixTimestampNanos => {
                                let ts =
                                    format!("{}-*", Timestamp::now_utc().unix_timestamp_nanos());
                                cmd.arg(&ts)
                            }
                        };
                        let msg = [(message_field.0, bytes)];
                        cmd.arg(&msg);
                        let command = (redis_key.to_owned(), stream_key, shard, receipt);
                        if batch.0.is_empty() || batch.0.last().unwrap().0 == command.0 {
                            batch.0.push(command);
                            batch.1.add_command(cmd);
                        } else {
                            next_batch.0.push(command);
                            next_batch.1.add_command(cmd);
                            break;
                        }
                    }
                }

                if batch.0.is_empty() {
                    batch = next_batch;
                    next_batch = Default::default();
                    continue;
                }

                let results: Vec<_> = if batch.0.first().unwrap().0 == SEA_STREAMER_INTERNAL {
                    vec![Ok(MessageHeader::new(
                        batch.0.first().unwrap().1.clone(),
                        batch.0.first().unwrap().2,
                        0,
                        Timestamp::now_utc(),
                    ))]
                } else {
                    let (mut retried, mut asked) = (0, 0);
                    let redis_key = &batch.0.first().unwrap().0;
                    loop {
                        let (node, conn) = match cluster.get_connection_for(redis_key).await {
                            Ok(conn) => conn,
                            Err(StreamErr::Backend(RedisErr::TryAgain(_))) => continue, // it will sleep inside `get_connection`
                            Err(err) => {
                                log::error!("{err:?}");
                                return; // this will kill the producer
                            }
                        };
                        match conn.req_packed_commands(&batch.1, 0, batch.0.len()).await {
                            Ok(ids) => {
                                assert_eq!(batch.0.len(), ids.len());
                                break ids
                                    .into_iter()
                                    .zip(batch.0.iter())
                                    .map(|(id, (_, stream_key, shard, _))| {
                                        match string_from_redis_value(id) {
                                            Ok(id) => match parse_message_id(timestamp_format, &id)
                                            {
                                                Ok((timestamp, sequence)) => {
                                                    Ok(MessageHeader::new(
                                                        stream_key.clone(),
                                                        *shard,
                                                        sequence,
                                                        timestamp,
                                                    ))
                                                }
                                                Err(err) => Err(err),
                                            },
                                            Err(err) => Err(err),
                                        }
                                    })
                                    .collect();
                            }
                            Err(err) => {
                                retried += 1;
                                if retried == MAX_RETRY {
                                    panic!(
                                    "The cluster might have a problem. Already retried {retried} times."
                                );
                                }
                                let kind = err.kind();
                                if kind == ErrorKind::Moved {
                                    cluster.moved(
                                        redis_key,
                                        match err.redirect_node() {
                                            Some((to, _slot)) => {
                                                // `to` must be in form of `host:port` without protocol
                                                format!("{}://{}", cluster.protocol().unwrap(), to)
                                                    .parse()
                                                    .expect("Failed to parse URL: {to}")
                                            }
                                            None => panic!("Key is moved, but to where? {err:?}"),
                                        },
                                    );
                                } else if is_ask(&kind) {
                                    // If it's an ASK, we wait until it finished moving.
                                    // This is an exponential backoff, in seq of [1, 2, 4, 8, 16, 32, 64].
                                    sleep(Duration::from_secs(1 << std::cmp::min(6, asked))).await;
                                    asked += 1;
                                } else if kind == ErrorKind::IoError {
                                    let node = node.to_owned();
                                    cluster.reconnect(&node).ok();
                                } else {
                                    // unrecoverable
                                    let err = match map_err(err) {
                                        StreamErr::Backend(err) => err,
                                        _ => unreachable!(),
                                    };
                                    break std::iter::repeat_with(|| {
                                        Err(StreamErr::Backend(err.clone()))
                                    })
                                    .collect();
                                }
                            }
                        }
                    }
                };

                remaining -= results.len();
                assert_eq!(batch.0.len(), results.len());
                for ((_, _, _, receipt), result) in batch.0.into_iter().zip(results.into_iter()) {
                    receipt.send_async(result).await.ok();
                }

                batch = next_batch;
                next_batch = Default::default();
            }
        }
    });

    Ok(RedisProducer {
        stream: None,
        sender,
    })
}

fn is_ask(kind: &ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::Ask | ErrorKind::TryAgain | ErrorKind::ClusterDown | ErrorKind::MasterDown
    )
}

impl PseudoRandomSharder {
    pub fn new(num_shards: u64) -> Self {
        Self { num_shards }
    }
}

impl SharderConfig for PseudoRandomSharder {
    fn init(&self) -> Box<dyn Sharder> {
        Box::new(self.clone())
    }
}

impl Sharder for PseudoRandomSharder {
    fn shard(&mut self, _: &StreamKey, _: &[u8]) -> u64 {
        Timestamp::now_utc().millisecond() as u64 % self.num_shards
    }
}

impl RoundRobinSharder {
    pub fn new(num_shards: u32) -> Self {
        Self {
            num_shards,
            state: 0,
        }
    }
}

impl SharderConfig for RoundRobinSharder {
    fn init(&self) -> Box<dyn Sharder> {
        Box::new(self.clone())
    }
}

impl Sharder for RoundRobinSharder {
    fn shard(&mut self, _: &StreamKey, _: &[u8]) -> u64 {
        let r = self.state % self.num_shards;
        self.state = self.state.wrapping_add(1);
        r as u64
    }
}
