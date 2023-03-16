mod cluster;
mod node;
mod options;
mod shard;

use cluster::*;
use node::*;
pub use options::*;
use shard::*;

use std::{sync::Arc, time::Duration};

use flume::{bounded, r#async::RecvStream, Receiver, Sender};
use sea_streamer_runtime::spawn_task;

use crate::{get_message_id, RedisCluster, RedisErr, RedisResult};
use sea_streamer_types::{
    export::{
        async_trait,
        futures::{future::BoxFuture, FutureExt},
    },
    Consumer, ConsumerGroup, ConsumerMode, Message, SeqPos, ShardId, SharedMessage, StreamErr,
    StreamKey, Timestamp,
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

pub type NextFuture<'a> = BoxFuture<'a, RedisResult<SharedMessage>>;

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
        async {
            let dead = || StreamErr::Backend(RedisErr::ConsumerDied);
            match self.receiver.recv_async().await {
                Ok(Ok(msg)) => {
                    if self.options.auto_commit() == &AutoCommit::Delayed {
                        self.handle
                            .send_async(CtrlMsg::Ack(
                                (msg.stream_key(), msg.shard_id()),
                                get_message_id(msg.header()),
                                Timestamp::now_utc(),
                            ))
                            .await
                            .map_err(|_| dead())?;
                    }
                    Ok(msg)
                }
                Ok(Err(err)) => Err(err),
                Err(_) => Err(dead()),
            }
        }
        .boxed()
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        todo!()
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
    let cluster = Cluster::new(
        uri,
        connect_options,
        consumer_options.clone(),
        shards,
        sender,
    )?;

    let (handle, response) = bounded(1024);
    spawn_task(cluster.run(response));

    Ok(RedisConsumer {
        options: consumer_options,
        receiver,
        handle,
    })
}
