use flume::{bounded, r#async::RecvFut, unbounded, Sender};
use redis::{aio::ConnectionLike, cmd as command};
use std::{fmt::Debug, future::Future, sync::Arc};

use crate::{map_err, parse_message_id, string_from_redis_value, RedisErr, RedisResult, MSG, ZERO};
use sea_streamer_runtime::spawn_task;
use sea_streamer_types::{
    export::{async_trait, futures::FutureExt},
    Buffer, MessageHeader, Producer, ProducerOptions, Receipt, ShardId, StreamErr, StreamKey,
    Timestamp,
};

const SEA_STREAMER_INTERNAL: &str = "SEA_STREAMER_INTERNAL";

#[derive(Debug, Clone)]
pub struct RedisProducer {
    stream: Option<StreamKey>,
    sender: Sender<SendRequest>,
}

#[derive(Default, Clone)]
pub struct RedisProducerOptions {
    sharder: Option<Arc<dyn SharderConfig>>,
}

impl Debug for RedisProducerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisProducerOptions")
            .field("sharder", &self.sharder.as_ref())
            .finish()
    }
}

struct SendRequest {
    stream_key: StreamKey,
    bytes: Vec<u8>,
    receipt: Sender<RedisResult<Receipt>>,
}

pub struct SendFuture {
    fut: RecvFut<'static, RedisResult<Receipt>>,
}

impl Debug for SendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFuture").finish()
    }
}

/// Struct to bootstrap sharders
pub trait SharderConfig: Debug + Send + Sync {
    /// Each producer will create its own sharder.
    /// They should not have any shared state for the sake of concurrency.
    fn init(&self) -> Box<dyn Sharder>;
}

/// Custom sharding strategy
pub trait Sharder: Send {
    /// Return the determined shard id for the given message.
    /// It will then be sent to the stream `STREAM_KEY:{SHARD}`, i.e. the shard becomes the hash tag.
    /// This will ensure that shards will be properly sharded by Redis Cluster.
    /// This should be a *real quick* computation, otherwise this can become the bottleneck of streaming.
    /// Mutex, atomic or anything that can create contention will be disastrous.
    fn shard(&mut self, stream_key: &StreamKey, bytes: &[u8]) -> u64;
}

#[derive(Debug)]
/// Shard streams pseudo-randomly but fairly. Basically a `rand() / num_shards`.
pub struct PseudoRandomSharder {
    num_shards: u64,
}

#[async_trait]
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
    async fn flush(self) -> RedisResult<()> {
        self.flush_once().await
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

impl RedisProducer {
    /// Like [`ProducerTrait::flush`], but does not destroy one self.
    pub async fn flush_once(&self) -> RedisResult<()> {
        // The trick here is to send a special message and wait for the receipt.
        // By the time it returns a receipt, everything before should have already been sent.
        let null = [];
        self.send_to(&StreamKey::new(SEA_STREAMER_INTERNAL)?, null.as_slice())?
            .await?;
        Ok(())
    }
}

impl ProducerOptions for RedisProducerOptions {}

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
    mut conn: redis::aio::Connection,
    mut options: RedisProducerOptions,
) -> RedisResult<RedisProducer> {
    let (sender, receiver) = unbounded();
    let mut sharder = options.sharder.take().map(|a| a.init());

    // Redis commands are exclusive (`&mut self`), so we need a producer task
    spawn_task(async move {
        // exit if all senders have been dropped
        while let Ok(SendRequest {
            stream_key,
            bytes,
            receipt,
        }) = receiver.recv_async().await
        {
            if stream_key.name() == SEA_STREAMER_INTERNAL && bytes.is_empty() {
                receipt
                    .send_async(Ok(MessageHeader::new(
                        stream_key,
                        ZERO,
                        0,
                        Timestamp::now_utc(),
                    )))
                    .await
                    .ok();
            } else {
                let mut cmd = command("XADD");
                let shard = if let Some(sharder) = sharder.as_mut() {
                    let shard = sharder.shard(&stream_key, bytes.as_slice());
                    cmd.arg(format!(
                        "{name}:{{{i}}}",
                        name = stream_key.name(),
                        i = shard
                    ));
                    ShardId::new(shard)
                } else {
                    cmd.arg(stream_key.name());
                    ZERO
                };
                cmd.arg("*");
                let msg = [(MSG, bytes)];
                cmd.arg(&msg);
                receipt
                    .send_async(match conn.req_packed_command(&cmd).await {
                        Ok(id) => match string_from_redis_value(id) {
                            Ok(id) => match parse_message_id(&id) {
                                Ok((timestamp, sequence)) => {
                                    Ok(MessageHeader::new(stream_key, shard, sequence, timestamp))
                                }
                                Err(err) => Err(err),
                            },
                            Err(err) => Err(err),
                        },
                        Err(err) => Err(map_err(err)),
                    })
                    .await
                    .ok();
            }
        }
    });

    Ok(RedisProducer {
        stream: None,
        sender,
    })
}

impl SharderConfig for PseudoRandomSharder {
    fn init(&self) -> Box<dyn Sharder> {
        let new = Self {
            num_shards: self.num_shards,
        };
        Box::new(new)
    }
}

impl Sharder for PseudoRandomSharder {
    fn shard(&mut self, _: &StreamKey, _: &[u8]) -> u64 {
        Timestamp::now_utc().millisecond() as u64 % self.num_shards
    }
}
