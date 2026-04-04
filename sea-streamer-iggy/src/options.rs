use sea_streamer_types::{
    ConnectOptions, ConsumerGroup, ConsumerMode, ConsumerOptions, ProducerOptions, StreamErr,
    StreamResult,
};
use std::time::Duration;

use crate::IggyErr;

#[derive(Debug, Clone)]
pub struct IggyConnectOptions {
    url: String,
    username: String,
    password: String,
    timeout: Option<Duration>,
}

impl Default for IggyConnectOptions {
    fn default() -> Self {
        Self {
            url: format!("iggy://localhost:{IGGY_PORT}"),
            username: "iggy".to_owned(),
            password: "iggy".to_owned(),
            timeout: None,
        }
    }
}

pub const IGGY_PORT: u16 = 8090;

impl IggyConnectOptions {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn set_url(&mut self, url: impl Into<String>) {
        self.url = url.into();
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn set_credentials(&mut self, username: impl Into<String>, password: impl Into<String>) {
        self.username = username.into();
        self.password = password.into();
    }

    pub fn password(&self) -> &str {
        &self.password
    }
}

impl ConnectOptions for IggyConnectOptions {
    type Error = IggyErr;

    fn timeout(&self) -> StreamResult<Duration, IggyErr> {
        match self.timeout {
            Some(d) => Ok(d),
            None => Err(StreamErr::TimeoutNotSet),
        }
    }

    fn set_timeout(&mut self, d: Duration) -> StreamResult<&mut Self, IggyErr> {
        self.timeout = Some(d);
        Ok(self)
    }
}

/// Where to start consuming messages from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IggyPollingStrategy {
    /// Start from a specific offset.
    Offset(u64),
    /// Start from a specific timestamp (Unix epoch in microseconds).
    Timestamp(u64),
    /// Start from the first message in the partition.
    First,
    /// Start from the last message in the partition.
    Last,
    /// Continue from the next unconsumed message.
    #[default]
    Next,
}

/// When to auto-commit consumed messages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IggyAutoCommit {
    /// Disable auto-commit; the consumer must commit manually.
    #[default]
    Disabled,
    /// Commit after every poll.
    AfterPolling,
    /// Commit at a fixed interval (in milliseconds).
    Interval(u64),
    /// Commit at a fixed interval (in milliseconds) or after every poll, whichever comes first.
    IntervalOrAfterPolling(u64),
}

#[derive(Debug, Clone)]
pub struct IggyConsumerOptions {
    mode: ConsumerMode,
    consumer_group: Option<ConsumerGroup>,
    stream_name: Option<String>,
    topic_name: Option<String>,
    batch_size: u32,
    polling_interval_ms: u64,
    consumer_name: Option<String>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    partitions_count: u32,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    polling_strategy: IggyPollingStrategy,
    auto_commit: IggyAutoCommit,
    polling_retry_interval_ms: u64,
    init_retries: u32,
    init_interval_ms: u64,
}

impl Default for IggyConsumerOptions {
    fn default() -> Self {
        Self {
            mode: ConsumerMode::default(),
            consumer_group: None,
            stream_name: None,
            topic_name: None,
            batch_size: 100,
            polling_interval_ms: 100,
            consumer_name: None,
            create_stream_if_not_exists: true,
            create_topic_if_not_exists: true,
            partitions_count: 1,
            auto_join_consumer_group: true,
            create_consumer_group_if_not_exists: true,
            polling_strategy: IggyPollingStrategy::default(),
            auto_commit: IggyAutoCommit::default(),
            polling_retry_interval_ms: 1000,
            init_retries: 3,
            init_interval_ms: 1000,
        }
    }
}

impl IggyConsumerOptions {
    pub fn stream_name(&self) -> Option<&str> {
        self.stream_name.as_deref()
    }

    pub fn set_stream_name(&mut self, name: impl Into<String>) {
        self.stream_name = Some(name.into());
    }

    pub fn topic_name(&self) -> Option<&str> {
        self.topic_name.as_deref()
    }

    pub fn set_topic_name(&mut self, name: impl Into<String>) {
        self.topic_name = Some(name.into());
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    pub fn set_batch_size(&mut self, size: u32) {
        self.batch_size = size;
    }

    pub fn polling_interval_ms(&self) -> u64 {
        self.polling_interval_ms
    }

    pub fn set_polling_interval_ms(&mut self, ms: u64) {
        self.polling_interval_ms = ms;
    }

    pub fn consumer_name(&self) -> Option<&str> {
        self.consumer_name.as_deref()
    }

    pub fn set_consumer_name(&mut self, name: impl Into<String>) {
        self.consumer_name = Some(name.into());
    }

    pub fn create_stream_if_not_exists(&self) -> bool {
        self.create_stream_if_not_exists
    }

    pub fn set_create_stream_if_not_exists(&mut self, val: bool) {
        self.create_stream_if_not_exists = val;
    }

    pub fn create_topic_if_not_exists(&self) -> bool {
        self.create_topic_if_not_exists
    }

    pub fn set_create_topic_if_not_exists(&mut self, val: bool) {
        self.create_topic_if_not_exists = val;
    }

    pub fn partitions_count(&self) -> u32 {
        self.partitions_count
    }

    pub fn set_partitions_count(&mut self, count: u32) {
        self.partitions_count = count;
    }

    pub fn auto_join_consumer_group(&self) -> bool {
        self.auto_join_consumer_group
    }

    pub fn set_auto_join_consumer_group(&mut self, val: bool) {
        self.auto_join_consumer_group = val;
    }

    pub fn create_consumer_group_if_not_exists(&self) -> bool {
        self.create_consumer_group_if_not_exists
    }

    pub fn set_create_consumer_group_if_not_exists(&mut self, val: bool) {
        self.create_consumer_group_if_not_exists = val;
    }

    pub fn polling_strategy(&self) -> &IggyPollingStrategy {
        &self.polling_strategy
    }

    pub fn set_polling_strategy(&mut self, strategy: IggyPollingStrategy) {
        self.polling_strategy = strategy;
    }

    pub fn auto_commit(&self) -> &IggyAutoCommit {
        &self.auto_commit
    }

    pub fn set_auto_commit(&mut self, auto_commit: IggyAutoCommit) {
        self.auto_commit = auto_commit;
    }

    pub fn polling_retry_interval_ms(&self) -> u64 {
        self.polling_retry_interval_ms
    }

    pub fn set_polling_retry_interval_ms(&mut self, ms: u64) {
        self.polling_retry_interval_ms = ms;
    }

    pub fn init_retries(&self) -> u32 {
        self.init_retries
    }

    pub fn set_init_retries(&mut self, retries: u32) {
        self.init_retries = retries;
    }

    pub fn init_interval_ms(&self) -> u64 {
        self.init_interval_ms
    }

    pub fn set_init_interval_ms(&mut self, ms: u64) {
        self.init_interval_ms = ms;
    }
}

impl ConsumerOptions for IggyConsumerOptions {
    type Error = IggyErr;

    fn new(mode: ConsumerMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    fn mode(&self) -> StreamResult<&ConsumerMode, IggyErr> {
        Ok(&self.mode)
    }

    fn consumer_group(&self) -> StreamResult<&ConsumerGroup, IggyErr> {
        match &self.consumer_group {
            Some(group) => Ok(group),
            None => Err(StreamErr::ConsumerGroupNotSet),
        }
    }

    fn set_consumer_group(
        &mut self,
        group_id: ConsumerGroup,
    ) -> StreamResult<&mut Self, IggyErr> {
        self.consumer_group = Some(group_id);
        Ok(self)
    }
}

/// How messages are distributed across partitions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IggyPartitioning {
    /// Balanced (round-robin) distribution.
    #[default]
    Balanced,
    /// Send all messages to a specific partition.
    PartitionId(u32),
    /// Partition by message key.
    MessageKey(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct IggyProducerOptions {
    stream_name: Option<String>,
    topic_name: Option<String>,
    batch_size: u32,
    send_interval_ms: u64,
    partitions_count: u32,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_replication_factor: Option<u32>,
    partitioning: IggyPartitioning,
    send_retries_count: u32,
    send_retries_interval_ms: u64,
}

impl Default for IggyProducerOptions {
    fn default() -> Self {
        Self {
            stream_name: None,
            topic_name: None,
            batch_size: 100,
            send_interval_ms: 5,
            partitions_count: 1,
            create_stream_if_not_exists: true,
            create_topic_if_not_exists: true,
            topic_replication_factor: None,
            partitioning: IggyPartitioning::default(),
            send_retries_count: 3,
            send_retries_interval_ms: 1000,
        }
    }
}

impl IggyProducerOptions {
    pub fn stream_name(&self) -> Option<&str> {
        self.stream_name.as_deref()
    }

    pub fn set_stream_name(&mut self, name: impl Into<String>) {
        self.stream_name = Some(name.into());
    }

    pub fn topic_name(&self) -> Option<&str> {
        self.topic_name.as_deref()
    }

    pub fn set_topic_name(&mut self, name: impl Into<String>) {
        self.topic_name = Some(name.into());
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    pub fn set_batch_size(&mut self, size: u32) {
        self.batch_size = size;
    }

    pub fn send_interval_ms(&self) -> u64 {
        self.send_interval_ms
    }

    pub fn set_send_interval_ms(&mut self, ms: u64) {
        self.send_interval_ms = ms;
    }

    pub fn partitions_count(&self) -> u32 {
        self.partitions_count
    }

    pub fn set_partitions_count(&mut self, count: u32) {
        self.partitions_count = count;
    }

    pub fn create_stream_if_not_exists(&self) -> bool {
        self.create_stream_if_not_exists
    }

    pub fn set_create_stream_if_not_exists(&mut self, val: bool) {
        self.create_stream_if_not_exists = val;
    }

    pub fn create_topic_if_not_exists(&self) -> bool {
        self.create_topic_if_not_exists
    }

    pub fn set_create_topic_if_not_exists(&mut self, val: bool) {
        self.create_topic_if_not_exists = val;
    }

    pub fn topic_replication_factor(&self) -> Option<u32> {
        self.topic_replication_factor
    }

    pub fn set_topic_replication_factor(&mut self, factor: u32) {
        self.topic_replication_factor = Some(factor);
    }

    pub fn partitioning(&self) -> &IggyPartitioning {
        &self.partitioning
    }

    pub fn set_partitioning(&mut self, partitioning: IggyPartitioning) {
        self.partitioning = partitioning;
    }

    pub fn send_retries_count(&self) -> u32 {
        self.send_retries_count
    }

    pub fn set_send_retries_count(&mut self, count: u32) {
        self.send_retries_count = count;
    }

    pub fn send_retries_interval_ms(&self) -> u64 {
        self.send_retries_interval_ms
    }

    pub fn set_send_retries_interval_ms(&mut self, ms: u64) {
        self.send_retries_interval_ms = ms;
    }
}

impl ProducerOptions for IggyProducerOptions {}
