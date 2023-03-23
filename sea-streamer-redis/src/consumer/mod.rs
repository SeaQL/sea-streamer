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

use crate::{get_message_id, host_id, RedisCluster, RedisErr, RedisResult, DEFAULT_TIMEOUT};
use sea_streamer_runtime::{spawn_task, timeout};
use sea_streamer_types::{
    export::async_trait, ConnectOptions, Consumer, ConsumerGroup, ConsumerId, ConsumerMode,
    ConsumerOptions, Message, SeqPos, ShardId, SharedMessage, StreamErr, StreamKey, Timestamp,
};

#[derive(Debug)]
pub struct RedisConsumer {
    config: ConsumerConfig,
    streams: Vec<(StreamKey, ShardId)>,
    receiver: Receiver<RedisResult<SharedMessage>>,
    handle: Sender<CtrlMsg>,
}

#[derive(Debug, Clone)]
pub struct RedisConsumerOptions {
    mode: ConsumerMode,
    group_id: Option<ConsumerGroup>,
    consumer_id: Option<ConsumerId>,
    consumer_timeout: Option<Duration>,
    auto_stream_reset: AutoStreamReset,
    auto_commit: AutoCommit,
    auto_commit_delay: Duration,
    auto_commit_interval: Duration,
    auto_claim_interval: Option<Duration>,
    auto_claim_idle: Duration,
    batch_size: usize,
    shard_ownership: ShardOwnership,
}

#[derive(Debug)]
struct ConsumerConfig {
    group_id: Option<ConsumerGroup>,
    consumer_id: Option<ConsumerId>,
    auto_ack: bool,
    pre_fetch: bool,
}

pub mod constants {
    use std::time::Duration;

    pub const DEFAULT_AUTO_COMMIT_DELAY: Duration = Duration::from_secs(5);
    pub const DEFAULT_AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(1);
    pub const DEFAULT_AUTO_CLAIM_INTERVAL: Duration = Duration::from_secs(30);
    pub const DEFAULT_AUTO_CLAIM_IDLE: Duration = Duration::from_secs(60);
    pub const DEFAULT_BATCH_SIZE: usize = 100;
    pub const DEFAULT_LOAD_BALANCED_BATCH_SIZE: usize = 10;
    #[cfg(feature = "test")]
    pub const HEARTBEAT: Duration = Duration::from_secs(1);
    #[cfg(not(feature = "test"))]
    pub const HEARTBEAT: Duration = Duration::from_secs(10);
}

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

    fn assign(&mut self, (stream, shard): (StreamKey, ShardId)) -> RedisResult<()> {
        if !self.streams.iter().any(|(s, _)| s == &stream) {
            return Err(StreamErr::StreamKeyNotFound);
        }
        if !self
            .streams
            .iter()
            .any(|(s, t)| (s, t) == (&stream, &shard))
        {
            self.streams.push((stream, shard));
        }
        Ok(())
    }

    fn unassign(&mut self, s: (StreamKey, ShardId)) -> RedisResult<()> {
        if let Some((i, _)) = self.streams.iter().enumerate().find(|(_, t)| &s == *t) {
            self.streams.remove(i);
            if self.streams.is_empty() {
                Err(StreamErr::StreamKeyEmpty)
            } else {
                Ok(())
            }
        } else {
            Err(StreamErr::StreamKeyNotFound)
        }
    }

    fn next(&self) -> NextFuture<'_> {
        NextFuture {
            con: self,
            fut: self.receiver.recv_async(),
            read: false,
        }
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        StreamFuture::new(self)
    }
}

impl RedisConsumer {
    pub fn group_id(&self) -> Option<&ConsumerGroup> {
        self.config.group_id.as_ref()
    }

    pub fn consumer_id(&self) -> Option<&ConsumerId> {
        self.config.consumer_id.as_ref()
    }

    /// Return the stream-shards this consumer has been assigned.
    /// On create, it will self-assign all shards.
    pub fn stream_shards(&self) -> &[(StreamKey, ShardId)] {
        &self.streams
    }

    #[inline]
    /// Mark a message as read. The ACK will be queued for commit.
    pub fn ack(&self, msg: &SharedMessage) -> RedisResult<()> {
        if self.config.auto_ack {
            return Err(StreamErr::Backend(RedisErr::InvalidClientConfig(
                "Please do not set AutoCommit to Delayed.".to_owned(),
            )));
        }
        self.auto_ack(msg)
    }

    fn auto_ack(&self, msg: &SharedMessage) -> RedisResult<()> {
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
    mut options: RedisConsumerOptions,
    streams: Vec<StreamKey>,
) -> RedisResult<RedisConsumer> {
    let mode = *options.mode()?;
    if mode != ConsumerMode::RealTime {
        if options.consumer_group().is_err() {
            options.set_consumer_group(group_id(&mode))?;
        }
        if options.consumer_id().is_none() {
            options.set_consumer_id(match mode {
                ConsumerMode::Resumable => ConsumerId::new(options.consumer_group()?.name()),
                ConsumerMode::LoadBalanced => consumer_id(),
                _ => unreachable!(),
            });
        }
    }

    if options.shard_ownership() == &ShardOwnership::Owned {
        todo!("Hopefully this will come out in the next release.");
    }

    let options = Arc::new(options);
    conn.reconnect_all().await?;
    let mut shards = Vec::new();
    for stream in streams {
        shards.extend(discover_shards(&mut conn, stream).await?);
    }
    let stream_shards = shards.iter().map(|s| s.stream.clone()).collect();

    let dur = conn.options.timeout().unwrap_or(DEFAULT_TIMEOUT);
    let enable_cluster = conn.options.enable_cluster();
    let config: ConsumerConfig = options.as_ref().into();
    let (sender, receiver) = if config.pre_fetch {
        // With pre-fetch, it will only read more if the channel is free.
        // Zero-capacity channels are always blocking. It means that *at the moment* the consumer
        // consumes the last item in the buffer, it will proceed to fetch more.
        // This number could be made configurable in the future.
        bounded(0)
    } else {
        // Without pre-fetch, it only fetches when next is called, aka. on demand.
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
            streams: stream_shards,
            receiver,
            handle,
        }),
        _ => Err(StreamErr::Connect(format!(
            "Failed to initialize {}",
            if enable_cluster { "cluster" } else { "node" }
        ))),
    }
}

pub fn group_id(mode: &ConsumerMode) -> ConsumerGroup {
    let id = format!(
        "{}:{}",
        host_id(),
        match mode {
            ConsumerMode::RealTime => "!",
            ConsumerMode::Resumable => "r",
            ConsumerMode::LoadBalanced => "s",
        }
    );
    ConsumerGroup::new(id)
}

pub fn consumer_id() -> ConsumerId {
    let thread_id = format!("{:?}", std::thread::current().id());
    let thread_id = thread_id
        .trim_start_matches("ThreadId(")
        .trim_end_matches(')');
    let id = format!(
        "{}:{}:{}:{}",
        host_id(),
        std::process::id(),
        thread_id,
        Timestamp::now_utc().unix_timestamp_nanos(),
    );
    ConsumerId::new(id)
}
