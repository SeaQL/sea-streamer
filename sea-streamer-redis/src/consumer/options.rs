use super::{constants::*, ConsumerConfig, RedisConsumerOptions};
use crate::{RedisErr, RedisResult};
use sea_streamer_types::{ConsumerGroup, ConsumerId, ConsumerMode, ConsumerOptions, StreamErr};
use std::time::Duration;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Where to start streaming from if there is no priori state.
pub enum AutoStreamReset {
    /// Use `0` as ID, which is the earliest message.
    Earliest,
    /// Use `$` as ID, which is the latest message.
    Latest,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// The auto ack / commit mechanism.
pub enum AutoCommit {
    /// `XREAD` with `NOACK`. This acknowledges messages as soon as they are fetched.
    /// In the event of service restart, this will likely result in messages being skipped.
    Immediate,
    /// Auto ack and commit, but only after `auto_commit_delay` has passed since messages are read.
    ///
    /// This is the default option, because users don't have to do anything extra. This also mimicks Kafka clients.
    Delayed,
    /// Do not auto ack, but continually commit acked messages to the server as new messages are read.
    /// The consumer will not commit more often than `auto_commit_interval`.
    /// You have to call [`RedisConsumer::ack`](crate::RedisConsumer::ack) manually.
    ///
    /// This is the recommended option for achieving 'at-least-once' semantics.
    Rolling,
    /// Never auto ack or commit.
    /// You have to call [`RedisConsumer::ack`](crate::RedisConsumer::ack) and [`RedisConsumer::commit`](crate::RedisConsumer::commit) manually.
    Disabled,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// The shard ownership model.
pub enum ShardOwnership {
    /// Consumers in the same group share the same shard
    Shared,
    /// Consumers claim ownership of a shard
    ///
    /// > This feature is still WIP
    Owned,
}

impl Default for RedisConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

impl From<&RedisConsumerOptions> for ConsumerConfig {
    fn from(options: &RedisConsumerOptions) -> Self {
        Self {
            group_id: options.consumer_group().ok().cloned(),
            consumer_id: options.consumer_id().cloned(),
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
            group_id: None,
            consumer_id: None,
            consumer_timeout: None,
            auto_stream_reset: AutoStreamReset::Latest,
            auto_commit: AutoCommit::Delayed,
            auto_commit_delay: DEFAULT_AUTO_COMMIT_DELAY,
            auto_commit_interval: DEFAULT_AUTO_COMMIT_INTERVAL,
            auto_claim_interval: Some(DEFAULT_AUTO_CLAIM_INTERVAL),
            auto_claim_idle: DEFAULT_AUTO_CLAIM_IDLE,
            batch_size: if mode == ConsumerMode::LoadBalanced {
                DEFAULT_LOAD_BALANCED_BATCH_SIZE
            } else {
                DEFAULT_BATCH_SIZE
            },
            shard_ownership: ShardOwnership::Shared,
            mkstream: false
        }
    }

    fn mode(&self) -> RedisResult<&ConsumerMode> {
        Ok(&self.mode)
    }

    fn consumer_group(&self) -> RedisResult<&ConsumerGroup> {
        self.group_id.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    /// SeaStreamer Redis offers two load-balancing mechanisms:
    ///
    /// ### (Fine-grained) Shared shard
    ///
    /// Multiple consumers in the same group can share the same shard.
    /// This is load-balanced in a first-ask-first-served manner, according to the Redis documentation.
    /// This can be considered dynamic load-balancing: faster consumers will consume more messages.
    ///
    /// This is the vanilla Redis consumer group behaviour.
    ///
    /// ### (Coarse) Owned shard
    ///
    /// > This feature is still WIP
    ///
    /// Multiple consumers within the same group do not share a shard.
    /// Each consumer will attempt to claim ownership of a shard, and other consumers will not step in.
    /// However, if a consumer has been idle for too long (defined by `auto_claim_idle`),
    /// another consumer will step in and claim ownership of the shard.
    ///
    /// As new consumers join the group, if the shard distribution is not in a 'fair state' (at most 1 shard
    /// per consumer), ownership of a shard may be transferred among consumers.
    ///
    /// This mimicks Kafka's consumer group behaviour.
    ///
    /// This is implemented via a dedicated key (for each stream) in the same Redis db,
    /// and managed co-operatively among consumers.
    fn set_consumer_group(&mut self, group_id: ConsumerGroup) -> RedisResult<&mut Self> {
        self.group_id = Some(group_id);
        Ok(self)
    }
}

impl RedisConsumerOptions {
    /// Unlike Kafka, Redis requires consumers to self-assign consumer IDs.
    /// If unset, SeaStreamer uses a combination of `host id` + `process id` + `thread id` + `timestamp`.
    pub fn consumer_id(&self) -> Option<&ConsumerId> {
        self.consumer_id.as_ref()
    }
    pub fn set_consumer_id(&mut self, consumer_id: ConsumerId) -> &mut Self {
        self.consumer_id = Some(consumer_id);
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
    /// If unset, defaults to `Latest`.
    pub fn set_auto_stream_reset(&mut self, v: AutoStreamReset) -> &mut Self {
        self.auto_stream_reset = v;
        self
    }
    pub fn auto_stream_reset(&self) -> &AutoStreamReset {
        &self.auto_stream_reset
    }

    /// There are two moving parts: auto-ack and auto-commit. You can enable one, or both.
    /// You can also opt out.
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

    /// The minimum interval for checking the XPENDING of others in the group.
    /// This option is only relevant when `mode` is `LoadBalanced`.
    ///
    /// Defaults to [`DEFAULT_AUTO_CLAIM_INTERVAL`]. None means never.
    pub fn set_auto_claim_interval(&mut self, v: Option<Duration>) -> &mut Self {
        self.auto_claim_interval = v;
        self
    }
    pub fn auto_claim_interval(&self) -> Option<&Duration> {
        self.auto_claim_interval.as_ref()
    }

    /// The idle time for a consumer to be considered dead and allow others to XCLAIM its messages.
    /// This option is only relevant when `mode` is `LoadBalanced`.
    ///
    /// Defaults to [`DEFAULT_AUTO_CLAIM_IDLE`]. None means never.
    pub fn set_auto_claim_idle(&mut self, v: Duration) -> &mut Self {
        self.auto_claim_idle = v;
        self
    }
    pub fn auto_claim_idle(&self) -> &Duration {
        &self.auto_claim_idle
    }

    /// Maximum number of messages to read from Redis in one request.
    /// A larger N would reduce the number of roundtrips.
    /// However, this also prevent messages from being chunked properly to load balance
    /// among consumers.
    ///
    /// Choose this number by considering the throughput of the stream, number of consumers
    /// in one group, and the time required to process each message.
    ///
    /// Cannot be `0`. If unset: if mode is `LoadBalanced`, defaults to [`DEFAULT_LOAD_BALANCED_BATCH_SIZE`].
    /// Otherwise, defaults to [`DEFAULT_BATCH_SIZE`].
    pub fn set_batch_size(&mut self, v: usize) -> &mut Self {
        assert_ne!(v, 0);
        self.batch_size = v;
        self
    }
    pub fn batch_size(&self) -> &usize {
        &self.batch_size
    }

    /// Default is `Shared`.
    pub fn shard_ownership(&self) -> &ShardOwnership {
        &self.shard_ownership
    }
    pub fn set_shard_ownership(&mut self, shard_ownership: ShardOwnership) -> &mut Self {
        self.shard_ownership = shard_ownership;
        self
    }

    /// By default, an `XGROUP CREATE <key> <groupname> <id or $>` command will be used to establish
    /// connection to the stream as part of a consumer group.
    /// If the stream key does not already exist, the consumer will fail to initialize. 
    /// 
    /// By setting this to `true`, a `XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM]` command will
    /// be used instead, allowing the consumer to initialize before the stream may be created 
    /// by the producer calling XADD when it first enqueues a message in the stream.
    pub fn set_mkstream(&mut self, enabled: bool) -> RedisResult<&mut Self> {
        self.mkstream = enabled;
        Ok(self)
    }

    pub fn mkstream(&self) -> RedisResult<bool> {
        Ok(self.mkstream)
    }

    /// Whether to pre-fetch the next page as the consumer is streaming, which results in less jitter.
    ///
    /// If false, it only fetches when [`Consumer::next`](sea_streamer_types::Consumer::next) is called, aka. on demand.
    ///
    /// This is a side effect of currently set consumer mode and auto commit options.
    /// e.g. when reading with `NOACK` the consumer should not pre-fetch.
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
