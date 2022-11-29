use crate::{Message, Result, SequenceNo, ShardId, Timestamp};
use async_trait::async_trait;
use futures::Stream;

#[derive(Debug)]
pub enum ConsumerMode {
    /// This is the 'vanilla' stream consumer. It does not commit, and only consumes messages from now on
    RealTime,
    /// When the process restarts, it will resume the stream from the previous committed sequence.
    /// It will use a consumer id unique to this host: on a physical machine, it will use the mac address.
    /// Inside a docker container, it will use the container id.
    Resumable,
    /// You should assign a consumer group manually. The load-balancing mechanism is implementation-specific.
    LoadBalanced,
}

#[derive(Debug)]
pub struct ConsumerGroup {
    name: String,
}

pub trait ConsumerOptions: Clone + Send {
    fn new(mode: ConsumerMode) -> Self;

    /// Get currently set consumer group; may return [`StreamErr::ConsumerGroupNotSet`].
    fn consumer_group(&self) -> Result<ConsumerGroup>;

    /// Set consumer group for this consumer. Note the semantic is implementation-specific.
    fn set_consumer_group(&mut self, group_id: ConsumerGroup) -> Result<()>;
}

#[async_trait]
pub trait Consumer: Sized + Send + Sync {
    type Stream<'a>: Stream<Item = Message<'a>>;

    /// Seek to an arbitrary point in time; start consuming the closest message
    fn seek(&self, to: Timestamp) -> Result<()>;

    /// Rewind the stream to a particular sequence number
    fn rewind(&self, seq: SequenceNo) -> Result<()>;

    /// Assign this consumer to a particular shard; This function can only be called once.
    /// Subsequent calls should return [`AlreadyAssigned`] error.
    fn assign(&self, shard: ShardId) -> Result<()>;

    /// Poll and receive one message: it awaits until there are new messages
    async fn next(&self) -> Result<Message>;

    /// Returns an async stream
    fn stream<'a>(self) -> Result<Self::Stream<'a>>;
}

impl ConsumerGroup {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
