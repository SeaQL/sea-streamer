mod cluster;
mod future;
mod node;
mod options;
mod shard;

use cluster::*;
use future::*;
use node::*;
pub use options::*;
use shard::*;

use flume::{bounded, unbounded, Receiver, Sender};
use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::{get_message_id, RedisCluster, RedisErr, RedisResult, DEFAULT_TIMEOUT};
use sea_streamer_runtime::{spawn_task, timeout};
use sea_streamer_types::{
    export::async_trait, ConnectOptions, Consumer, ConsumerGroup, ConsumerMode, Message, SeqPos,
    ShardId, SharedMessage, StreamErr, StreamKey, Timestamp,
};

#[derive(Debug)]
pub struct RedisConsumer {
    config: ConsumerConfig,
    receiver: Receiver<RedisResult<SharedMessage>>,
    handle: Sender<CtrlMsg>,
}

#[derive(Debug, Clone)]
pub struct RedisConsumerOptions {
    mode: ConsumerMode,
    group: Option<ConsumerGroup>,
    shared_shard: bool,
    consumer_timeout: Option<Duration>,
    auto_stream_reset: AutoStreamReset,
    auto_commit: AutoCommit,
    auto_commit_delay: Duration,
    auto_commit_interval: Duration,
}

#[derive(Debug)]
struct ConsumerConfig {
    auto_ack: bool,
    pre_fetch: bool,
}

pub const DEFAULT_AUTO_COMMIT_DELAY: Duration = Duration::from_secs(5);
pub const DEFAULT_AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(feature = "test")]
pub const HEARTBEAT: Duration = Duration::from_secs(1);
#[cfg(not(feature = "test"))]
pub const HEARTBEAT: Duration = Duration::from_secs(10);
/// Maximum number of messages to read from Redis in one operation
pub const BATCH_SIZE: usize = 100;

#[async_trait]
impl Consumer for RedisConsumer {
    type Error = RedisErr;
    type Message<'a> = SharedMessage;
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = StreamFuture<'a>;

    async fn seek(&mut self, _: Timestamp) -> RedisResult<()> {
        todo!()
    }

    fn rewind(&mut self, _: SeqPos) -> RedisResult<()> {
        todo!()
    }

    fn assign(&mut self, _: ShardId) -> RedisResult<()> {
        todo!()
    }

    fn next(&self) -> NextFuture<'_> {
        NextFuture {
            con: self,
            fut: self.receiver.recv_async(),
        }
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        StreamFuture::new(self)
    }
}

impl RedisConsumer {
    fn pending_read(&self) {
        if !self.config.pre_fetch {
            self.handle.try_send(CtrlMsg::Read).ok();
        }
    }

    #[inline]
    /// Mark a message as read. The ACK will be queued for commit.
    pub fn ack(&self, msg: &SharedMessage) -> RedisResult<()> {
        if self.config.auto_ack {
            return Err(StreamErr::Backend(RedisErr::InvalidClientConfig(
                "Please do not set AutoCommit to Delayed.".to_owned(),
            )));
        }
        self.ack_unchecked(msg)
    }

    fn ack_unchecked(&self, msg: &SharedMessage) -> RedisResult<()> {
        // unbounded, so never blocks
        if self
            .handle
            .try_send(CtrlMsg::Ack(
                (msg.stream_key(), msg.shard_id()),
                get_message_id(msg.header()),
                Timestamp::now_utc(),
            ))
            .is_ok()
        {
            Ok(())
        } else {
            Err(StreamErr::Backend(RedisErr::ConsumerDied))
        }
    }

    /// Commit all pending acks
    pub async fn commit(&mut self) -> RedisResult<()> {
        if self.config.pre_fetch {
            return Err(StreamErr::Backend(RedisErr::InvalidClientConfig(
                "Manual commit is not allowed. Please use another AutoCommit option.".to_owned(),
            )));
        }
        let (sender, notify) = bounded(1);
        if self
            .handle
            .send_async(CtrlMsg::Commit(sender))
            .await
            .is_ok()
        {
            notify.recv_async().await.ok();
        }
        Ok(())
    }

    /// Commit all pending acks and end the consumer
    pub async fn end(self) -> RedisResult<()> {
        let (sender, notify) = bounded(1);
        if self.handle.send_async(CtrlMsg::Kill(sender)).await.is_ok() {
            let receiver = self.receiver;
            // drain the channel
            spawn_task(async move { while receiver.recv_async().await.is_ok() {} });
            notify.recv_async().await.ok();
        }
        Ok(())
    }
}

pub(crate) async fn create_consumer(
    mut conn: RedisCluster,
    options: RedisConsumerOptions,
    streams: Vec<StreamKey>,
) -> RedisResult<RedisConsumer> {
    let options = Arc::new(options);
    conn.reconnect_all().await?;
    let mut shards = Vec::new();
    for stream in streams {
        shards.extend(discover_shards(&mut conn, stream).await?);
    }

    let dur = conn.options.timeout().unwrap_or(DEFAULT_TIMEOUT);
    let enable_cluster = conn.options.enable_cluster();
    let config: ConsumerConfig = options.as_ref().into();
    let (sender, receiver) = if config.pre_fetch {
        // with pre-fetch, it will only read more if the channel is free.
        // Zero-capacity channels are always blocking. It means that *at the moment* the consumer
        // consumes the last item in the buffer, it will proceed to fetch more.
        // This number could be made configurable in the future.
        bounded(0)
    } else {
        // without pre-fetch, it will only read if consumer reads
        unbounded()
    };
    let (handle, response) = unbounded();
    let (status, ready) = bounded(1);

    if enable_cluster {
        let cluster = Cluster::new(options.clone(), shards, sender)?;
        spawn_task(cluster.run(conn, response, status));
    } else {
        if conn.cluster.nodes().len() != 1 {
            return Err(StreamErr::Connect(
                "There are multiple nodes in streamer URI, please enable the cluster option"
                    .to_owned(),
            ));
        }
        let node = Node::new(conn, options.clone(), shards, handle.clone(), sender)?;
        spawn_task(node.run(response, status));
    }

    match timeout(dur, ready.recv_async()).await {
        Ok(Ok(StatusMsg::Ready)) => Ok(RedisConsumer {
            config,
            receiver,
            handle,
        }),
        _ => Err(StreamErr::Connect(format!(
            "Failed to initialize {}",
            if enable_cluster { "cluster" } else { "node" }
        ))),
    }
}
