use crate::{Message, SeqPos, ShardId, StreamKey, StreamResult, Timestamp};
use futures::{Future, Stream};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Mode of stream consumption.
pub enum ConsumerMode {
    /// This is the 'vanilla' stream consumer. It does not auto-commit, and thus only consumes messages from now on.
    RealTime,
    /// When the process restarts, it will resume the stream from the previous committed sequence.
    Resumable,
    /// You should assign a consumer group manually. The load-balancing mechanism is implementation-specific.
    LoadBalanced,
}

impl Default for ConsumerMode {
    fn default() -> Self {
        Self::RealTime
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Used to identify a group of consumers.
pub struct ConsumerGroup {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Used to identify a consumer within a group.
pub struct ConsumerId {
    id: String,
}

/// Common options of a Consumer.
pub trait ConsumerOptions: Default + Clone + Send {
    type Error: std::error::Error;

    fn new(mode: ConsumerMode) -> Self;

    /// Get currently set ConsumerMode
    fn mode(&self) -> StreamResult<&ConsumerMode, Self::Error>;

    /// Get currently set consumer group; may return `StreamErr::ConsumerGroupNotSet`.
    fn consumer_group(&self) -> StreamResult<&ConsumerGroup, Self::Error>;

    /// Set consumer group for this consumer. Note the semantic is implementation-specific.
    fn set_consumer_group(
        &mut self,
        group_id: ConsumerGroup,
    ) -> StreamResult<&mut Self, Self::Error>;
}

/// Common interface of consumers, to be implemented by all backends.
pub trait Consumer: Sized + Send + Sync {
    type Error: std::error::Error;

    type Message<'a>: Message
    where
        Self: 'a;
    type NextFuture<'a>: Future<Output = StreamResult<Self::Message<'a>, Self::Error>>
    where
        Self: 'a;
    type Stream<'a>: Stream<Item = StreamResult<Self::Message<'a>, Self::Error>>
    where
        Self: 'a;

    /// Seek all streams to an arbitrary point in time. It will start consuming from the earliest message
    /// with a timestamp later than `to`.
    ///
    /// If the consumer is not already assigned, shard ZERO will be used.
    fn seek(&mut self, to: Timestamp)
    -> impl Future<Output = StreamResult<(), Self::Error>> + Send;

    /// Rewind all streams to a particular sequence number.
    ///
    /// If the consumer is not already assigned, shard ZERO will be used.
    fn rewind(
        &mut self,
        offset: SeqPos,
    ) -> impl Future<Output = StreamResult<(), Self::Error>> + Send;

    /// Assign this consumer to a particular shard. Can be called multiple times to assign
    /// to multiple shards. Returns error `StreamKeyNotFound` if the stream is not currently subscribed.
    ///
    /// It will only take effect on the next [`Consumer::seek`] or [`Consumer::rewind`].
    fn assign(&mut self, ss: (StreamKey, ShardId)) -> StreamResult<(), Self::Error>;

    /// Unassign a shard. Returns error `StreamKeyNotFound` if the stream is not currently subscribed.
    /// Returns error `StreamKeyEmpty` if all streams have been unassigned.
    fn unassign(&mut self, ss: (StreamKey, ShardId)) -> StreamResult<(), Self::Error>;

    /// Poll and receive one message: it awaits until there are new messages.
    /// This method can be called from multiple threads.
    fn next(&self) -> Self::NextFuture<'_>;

    /// Returns an async stream. You cannot create multiple streams from the same consumer,
    /// nor perform any operation while streaming.
    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a>;
}

impl ConsumerGroup {
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ConsumerId {
    pub fn new<S: Into<String>>(id: S) -> Self {
        Self { id: id.into() }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}
