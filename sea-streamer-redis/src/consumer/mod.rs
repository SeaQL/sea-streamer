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
    options: Arc<RedisConsumerOptions>,
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
}

pub const DEFAULT_AUTO_COMMIT_DELAY: Duration = Duration::from_secs(5);
pub const HEARTBEAT: Duration = Duration::from_secs(10);
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
    fn ack(&self, msg: &SharedMessage) -> RedisResult<()> {
        // unbounded never blocks
        if self
            .handle
            .send(CtrlMsg::Ack(
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
}

pub(crate) async fn create_consumer(
    mut cluster: RedisCluster,
    consumer_options: RedisConsumerOptions,
    streams: Vec<StreamKey>,
) -> RedisResult<RedisConsumer> {
    let consumer_options = Arc::new(consumer_options);
    cluster.reconnect_all().await?;
    let (sender, receiver) = bounded(1);
    let mut shards = Vec::new();
    for stream in streams {
        shards.extend(discover_shards(&mut cluster, stream).await?);
    }

    let (uri, connect_options) = cluster.into_config();
    let dur = connect_options.timeout().unwrap_or(DEFAULT_TIMEOUT);
    let cluster = Cluster::new(
        uri,
        connect_options,
        consumer_options.clone(),
        shards,
        sender,
    )?;

    let (handle, response) = unbounded();
    let (status, ready) = bounded(1);
    spawn_task(cluster.run(response, status));

    match timeout(dur, ready.recv_async()).await {
        Ok(Ok(StatusMsg::Ready)) => Ok(RedisConsumer {
            options: consumer_options,
            receiver,
            handle,
        }),
        _ => Err(StreamErr::Connect(
            "Failed to initialize cluster".to_owned(),
        )),
    }
}
