use async_trait::async_trait;
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, MessageStream as RawMessageStream, StreamConsumer as RawConsumer},
    message::BorrowedMessage as RawMessage,
    Message as KafkaMessageTrait, Offset, TopicPartitionList,
};
use std::time::Duration;

use sea_streamer::{
    export::futures::{stream::Map as StreamMap, StreamExt},
    Consumer as ConsumerTrait, ConsumerGroup, ConsumerMode, ConsumerOptions, Message, Payload,
    SequenceNo, SequencePos, ShardId, StreamErr, StreamKey, StreamerUri, Timestamp,
};

use crate::{
    cluster_uri, impl_into_string, stream_err, KafkaConnectOptions, KafkaErr, KafkaResult,
};

pub struct KafkaConsumer {
    inner: RawConsumer,
    shard: Option<ShardId>,
    streams: Vec<StreamKey>,
}

pub struct KafkaMessage<'a> {
    mess: RawMessage<'a>,
}

#[derive(Debug, Default, Clone)]
pub struct KafkaConsumerOptions {
    pub(crate) mode: ConsumerMode,
    /// https://kafka.apache.org/documentation/#connectconfigs_group.id
    pub(crate) group_id: Option<ConsumerGroup>,
    /// https://kafka.apache.org/documentation/#connectconfigs_session.timeout.ms
    pub(crate) session_timeout: Option<Duration>,
    /// https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
    pub(crate) auto_offset_reset: Option<AutoOffsetReset>,
    /// https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit
    pub(crate) enable_auto_commit: Option<bool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OptionKey {
    BootstrapServers,
    GroupId,
    SessionTimeout,
    AutoOffsetReset,
    EnableAutoCommit,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// automatically reset the offset to the earliest offset
    Earliest,
    /// automatically reset the offset to the latest offset
    Latest,
    /// throw exception to the consumer if no previous offset is found for the consumer's group
    NoReset,
}

impl KafkaConsumerOptions {
    /// A unique string that identifies the consumer group this consumer belongs to.
    /// This property is required if the consumer uses either the group management functionality
    /// by using subscribe(topic) or the Kafka-based offset management strategy.
    pub fn set_group_id(&mut self, id: ConsumerGroup) -> &mut Self {
        self.group_id = Some(id);
        self
    }

    /// The timeout used to detect worker failures. The worker sends periodic heartbeats
    /// to indicate its liveness to the broker. If no heartbeats are received by the broker
    /// before the expiration of this session timeout, then the broker will remove the worker
    /// from the group and initiate a rebalance.
    pub fn set_session_timeout(&mut self, v: Duration) -> &mut Self {
        self.session_timeout = Some(v);
        self
    }

    /// What to do when there is no initial offset in Kafka or if the current offset does
    /// not exist any more on the server.
    pub fn set_auto_offset_reset(&mut self, v: AutoOffsetReset) -> &mut Self {
        self.auto_offset_reset = Some(v);
        self
    }

    pub fn set_enable_auto_commit(&mut self, v: bool) -> &mut Self {
        self.enable_auto_commit = Some(v);
        self
    }

    fn make_client_config(&self, client_config: &mut ClientConfig) {
        if let Some(group_id) = &self.group_id {
            client_config.set(OptionKey::GroupId, group_id.name());
        } else {
            // https://github.com/edenhill/librdkafka/issues/3261
            // librdkafka always require a group_id even when not joining a consumer group
            // But this is purely a client side issue
            client_config.set(OptionKey::GroupId, "abcdefg");
        }
        if let Some(v) = self.session_timeout {
            client_config.set(OptionKey::SessionTimeout, format!("{}", v.as_millis()));
        }
        if let Some(v) = self.auto_offset_reset {
            client_config.set(OptionKey::AutoOffsetReset, v);
        }
        if let Some(v) = self.enable_auto_commit {
            client_config.set(OptionKey::EnableAutoCommit, v.to_string());
        }
    }
}

impl OptionKey {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BootstrapServers => "bootstrap.servers",
            Self::GroupId => "group.id",
            Self::SessionTimeout => "session.timeout.ms",
            Self::AutoOffsetReset => "auto.offset.reset",
            Self::EnableAutoCommit => "enable.auto.commit",
        }
    }
}

impl AutoOffsetReset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::NoReset => "none",
        }
    }
}

impl_into_string!(OptionKey);
impl_into_string!(AutoOffsetReset);

impl std::fmt::Debug for KafkaConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConsumer").finish()
    }
}

#[async_trait]
impl ConsumerTrait for KafkaConsumer {
    type Error = KafkaErr;
    type Message<'a> = KafkaMessage<'a>;
    /// See, we don't actually have to Box this! Looking forward to `type_alias_impl_trait`
    type Stream<'a> = StreamMap<
        RawMessageStream<'a>,
        fn(Result<RawMessage<'a>, KafkaErr>) -> KafkaResult<KafkaMessage<'a>>,
    >;

    fn seek(&self, _: Timestamp) -> KafkaResult<()> {
        if self.shard.is_none() {
            Err(StreamErr::NotAssigned)
        } else {
            Err(StreamErr::Unsupported("KafkaConsumer::seek".to_owned()))
        }
    }

    fn rewind(&self, offset: SequencePos) -> KafkaResult<()> {
        let mut tpl = TopicPartitionList::new();

        if let Some(shard) = self.shard {
            for stream in self.streams.iter() {
                tpl.add_partition_offset(
                    stream.name(),
                    shard.id() as i32,
                    match offset {
                        SequencePos::Beginning => Offset::Beginning,
                        SequencePos::End => Offset::End,
                        SequencePos::At(seq) => Offset::Offset(
                            seq.try_into()
                                .expect("KafkaConsumer::rewind: u64 out of range"),
                        ),
                    },
                )
                .map_err(stream_err)?;
            }

            self.inner.assign(&tpl).map_err(stream_err)?;

            Ok(())
        } else {
            Err(StreamErr::Unsupported("KafkaConsumer::rewind".to_owned()))
        }
    }

    fn assign(&mut self, shard: ShardId) -> KafkaResult<()> {
        if self.shard.is_none() {
            self.shard = Some(shard);
            Ok(())
        } else {
            Err(StreamErr::AlreadyAssigned)
        }
    }

    async fn next<'a>(&'a self) -> KafkaResult<Self::Message<'a>> {
        Self::process(self.inner.recv().await)
    }

    fn stream<'a, 'b: 'a>(&'b self) -> Self::Stream<'a> {
        self.inner.stream().map(Self::process)
    }
}

impl KafkaConsumer {
    fn process(res: Result<RawMessage, KafkaErr>) -> KafkaResult<KafkaMessage> {
        match res {
            Ok(mess) => Ok(KafkaMessage { mess }),
            Err(err) => Err(StreamErr::Backend(err)),
        }
    }
}

impl<'a> Message for KafkaMessage<'a> {
    fn stream_key(&self) -> StreamKey {
        StreamKey::new(self.mess.topic().to_owned())
    }

    fn shard_id(&self) -> ShardId {
        ShardId::new(self.mess.partition() as u64)
    }

    fn sequence(&self) -> SequenceNo {
        self.mess.offset() as SequenceNo
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::from_unix_timestamp_nanos(
            self.mess
                .timestamp()
                .to_millis()
                .expect("message.timestamp() is None") as i128
                * 1_000_000,
        )
        .expect("from_unix_timestamp_nanos")
    }

    fn message(&self) -> Payload {
        Payload::new(self.mess.payload().unwrap_or_default())
    }
}

impl ConsumerOptions for KafkaConsumerOptions {
    type Error = KafkaErr;

    fn new(mode: ConsumerMode) -> Self {
        KafkaConsumerOptions {
            mode,
            ..Default::default()
        }
    }

    fn consumer_group(&self) -> KafkaResult<&ConsumerGroup> {
        self.group_id.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    /// If multiple consumers shares the same group, only one consumer in the group will
    /// receive a message, i.e. it is load-balanced.
    ///
    /// However, the load-balancing mechanism is what makes Kafka different:
    ///
    /// Each stream is divided into multiple shards (known as partition),
    /// and each partition will be assigned to only one consumer in a group.
    ///
    /// Say there are 2 consumers (in the group) and 2 partitions, then each consumer
    /// will receive messages from one partition, and they are thus load-balanced.
    ///
    /// However if the stream has only 1 partition, even if there are many consumers,
    /// these messages will only be received by the assigned consumer, and other consumers
    /// will be in stand-by mode, resulting in a hot-failover setup.
    fn set_consumer_group(&mut self, group: ConsumerGroup) -> KafkaResult<&mut Self> {
        self.group_id = Some(group);
        Ok(self)
    }
}

pub(crate) fn create_consumer(
    streamer: &StreamerUri,
    base_options: &KafkaConnectOptions,
    options: &KafkaConsumerOptions,
    streams: Vec<StreamKey>,
) -> Result<KafkaConsumer, KafkaErr> {
    let mut client_config = ClientConfig::new();
    client_config.set(OptionKey::BootstrapServers, cluster_uri(streamer));
    base_options.make_client_config(&mut client_config);
    options.make_client_config(&mut client_config);
    let consumer: RawConsumer = client_config.create()?;

    if !streams.is_empty() {
        let topics: Vec<&str> = streams.iter().map(|s| s.name()).collect();
        consumer.subscribe(&topics)?;
    } else {
        panic!("no topic?");
    }

    Ok(KafkaConsumer {
        inner: consumer,
        shard: None,
        streams,
    })
}
