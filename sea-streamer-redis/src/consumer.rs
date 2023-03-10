use flume::r#async::RecvStream;
use redis::{aio::ConnectionLike, cmd as command, streams::StreamReadOptions, ToRedisArgs};

use crate::{map_err, RedisErr, RedisResult};
use sea_streamer_types::{
    export::{async_trait, futures::future::Ready},
    Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions, SeqPos, ShardId, SharedMessage,
    StreamErr, Timestamp,
};

#[derive(Debug)]
pub struct StreamReadReply {
    pub messages: Vec<SharedMessage>,
}

#[derive(Debug)]
pub struct RedisConsumer {}

#[derive(Debug, Clone)]
pub struct RedisConsumerOptions {
    mode: ConsumerMode,
    group: Option<ConsumerGroup>,
}

pub async fn xread_options<C, K, ID>(
    con: &mut C,
    keys: &[K],
    ids: &[ID],
    options: &StreamReadOptions,
) -> RedisResult<StreamReadReply>
where
    K: ToRedisArgs,
    ID: ToRedisArgs,
    C: ConnectionLike,
{
    let mut cmd = command(if options.read_only() {
        "XREAD"
    } else {
        "XREADGROUP"
    });
    cmd.arg(options).arg("STREAMS").arg(keys).arg(ids);

    let value = con.req_packed_command(&cmd).await.map_err(map_err)?;

    StreamReadReply::from_redis_value(value)
}

pub type NextFuture = Ready<RedisResult<SharedMessage>>;

impl ConsumerOptions for RedisConsumerOptions {
    type Error = RedisErr;

    fn new(mode: ConsumerMode) -> Self {
        Self { mode, group: None }
    }

    fn mode(&self) -> RedisResult<&ConsumerMode> {
        Ok(&self.mode)
    }

    fn consumer_group(&self) -> RedisResult<&ConsumerGroup> {
        self.group.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    /// If multiple consumers share the same group, only one in the group will receive a message.
    /// This is load-balanced in a first-ask-first-served manner, according to the Redis documentation.
    /// This can be considered dynamic load-balancing: faster consumers will consume more messages.
    fn set_consumer_group(&mut self, group: ConsumerGroup) -> RedisResult<&mut Self> {
        self.group = Some(group);
        Ok(self)
    }
}

impl Default for RedisConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

#[async_trait]
impl Consumer for RedisConsumer {
    type Error = RedisErr;
    type Message<'a> = SharedMessage;
    // See we don't actually have to Box these! Looking forward to `type_alias_impl_trait`
    type NextFuture<'a> = NextFuture;
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
        todo!()
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        todo!()
    }
}
