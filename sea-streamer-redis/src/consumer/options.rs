use super::{RedisConsumerOptions, DEFAULT_AUTO_COMMIT_DELAY};
use crate::{RedisErr, RedisResult};
use sea_streamer_types::{ConsumerGroup, ConsumerMode, ConsumerOptions, StreamErr};
use std::time::Duration;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AutoStreamReset {
    /// Use `0` as ID, which is the earliest message.
    Earliest,
    /// Use `$` as ID, which is the latest message.
    Latest,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AutoCommit {
    /// `XREAD` with `NOACK`. This acknowledges messages as soon as they are fetched.
    Immediate,
    /// Auto commit, but only after `auto_commit_delay` has passed since messages are read.
    Delayed,
    /// Never auto ack or commit.
    Disabled,
}

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
            auto_commit: AutoCommit::Delayed,
            auto_commit_delay: DEFAULT_AUTO_COMMIT_DELAY,
        }
    }

    fn mode(&self) -> RedisResult<&ConsumerMode> {
        Ok(&self.mode)
    }

    /// ### Consumer ID
    ///
    /// Unlike Kafka, Redis requires consumers to self-assign consumer IDs.
    /// SeaStreamer uses a combination of `host id` + `process id` + `timestamp` when made is `LoadBalanced`.
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

    /// If you want to commit only what have been explicitly acked, set it to `Disabled`.
    ///
    /// If unset, defaults to `Delayed`.
    pub fn set_auto_commit(&mut self, v: AutoCommit) -> &mut Self {
        self.auto_commit = v;
        self
    }
    pub fn auto_commit(&self) -> &AutoCommit {
        &self.auto_commit
    }

    /// The interval for acks to be committed to the server.
    /// This option is only relevant when `auto_commit` is `Delayed`.
    ///
    /// If unset, defaults to [`DEFAULT_AUTO_COMMIT_DELAY`].
    pub fn set_auto_commit_delay(&mut self, v: Duration) -> &mut Self {
        self.auto_commit_delay = v;
        self
    }
    pub fn auto_commit_delay(&self) -> &Duration {
        &self.auto_commit_delay
    }
}
