use super::{
    ConsumerConfig, RedisConsumerOptions, DEFAULT_AUTO_COMMIT_DELAY, DEFAULT_AUTO_COMMIT_INTERVAL,
};
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
    /// In the event of service restart, this will likely result in messages being skipped.
    Immediate,
    /// Auto ack and commit, but only after `auto_commit_delay` has passed since messages are read.
    Delayed,
    /// Do not auto ack, but continually commit acked messages to the server as new messages are read.
    /// The consumer will not commit more often than `auto_commit_interval`.
    /// You have to call [`RedisConsumer::ack`] manually.
    Rolling,
    /// Never auto ack or commit.
    /// You have to call [`RedisConsumer::ack`] and [`RedisConsumer::commit`] manually.
    Disabled,
}

impl Default for RedisConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

impl From<&RedisConsumerOptions> for ConsumerConfig {
    fn from(options: &RedisConsumerOptions) -> Self {
        Self {
            auto_ack: options.auto_commit() == &AutoCommit::Delayed,
            pre_fetch: options.pre_fetch(),
        }
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
            auto_commit_interval: DEFAULT_AUTO_COMMIT_INTERVAL,
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

    /// The time needed for an ACK to realize.
    /// It is timed from the moment `next` returns.
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

    /// The minimum interval for acks to be committed to the server.
    /// This option is only relevant when `auto_commit` is `Rolling`.
    ///
    /// If unset, defaults to [`DEFAULT_AUTO_COMMIT_INTERVAL`].
    pub fn set_auto_commit_interval(&mut self, v: Duration) -> &mut Self {
        self.auto_commit_interval = v;
        self
    }
    pub fn auto_commit_interval(&self) -> &Duration {
        &self.auto_commit_interval
    }

    /// Whether pre-fetch the next page as you are streaming. This results in less jitter.
    /// This option is a side effects of consumer mode and auto_commit.
    pub fn pre_fetch(&self) -> bool {
        if self.mode == ConsumerMode::RealTime {
            true
        } else {
            matches!(
                self.auto_commit(),
                AutoCommit::Delayed | AutoCommit::Rolling
            )
        }
    }
}
