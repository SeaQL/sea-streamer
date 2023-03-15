use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};

use flume::{bounded, r#async::RecvStream, Receiver, Sender, TryRecvError};
use redis::{
    aio::ConnectionLike, cmd as command, streams::StreamReadOptions, AsyncCommands, ErrorKind,
    RedisWrite, ToRedisArgs, Value,
};
use sea_streamer_runtime::{sleep, spawn_task, timeout};

use crate::{
    get_message_id, host_id, map_err, Connection, MessageId, NodeId, RedisCluster,
    RedisConnectOptions, RedisErr, RedisResult, ZERO,
};
use sea_streamer_types::{
    export::{
        async_trait,
        futures::{future::BoxFuture, FutureExt},
    },
    Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions, Message, MessageHeader, SeqPos,
    ShardId, SharedMessage, StreamErr, StreamKey, StreamUrlErr, StreamerUri, Timestamp,
};

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StreamReadReply(pub(crate) Vec<SharedMessage>);

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

const DOLLAR: &str = "$";
const DIRECT: &str = ">";
const ONE_SEC: Duration = Duration::from_secs(1);

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

pub type NextFuture<'a> = BoxFuture<'a, RedisResult<SharedMessage>>;

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

pub type StreamShard = (StreamKey, ShardId);

enum CtrlMsg {
    AddShard(ShardState),
    Ack(StreamShard, MessageId, Timestamp),
}

enum ClusterEvent {
    Moved {
        shard: ShardState,
        from: NodeId,
        to: NodeId,
    },
}

struct Cluster {
    uri: StreamerUri,
    connect_options: Arc<RedisConnectOptions>,
    shards: Vec<ShardState>,
    consumer_options: Arc<RedisConsumerOptions>,
    messages: Sender<RedisResult<SharedMessage>>,
    nodes: HashMap<NodeId, Sender<CtrlMsg>>,
    keys: HashMap<StreamShard, NodeId>,
}

struct Node {
    id: NodeId,
    connect_options: Arc<RedisConnectOptions>,
    consumer_options: Arc<RedisConsumerOptions>,
    shards: Vec<ShardState>,
    messages: Sender<RedisResult<SharedMessage>>,
    opts: StreamReadOptions,
    group: GroupState,
}

#[derive(Debug)]
struct GroupState {
    group_id: String,
    first_read: bool,
    pending_state: bool,
}

#[derive(Debug)]
struct ShardState {
    stream: StreamShard,
    key: String,
    id: Option<MessageId>,
    pending_ack: Vec<PendingAck>,
}

#[derive(Debug, Clone, Copy)]
struct PendingAck(MessageId, Timestamp);

#[repr(transparent)]
struct AckDisplay<'a>(&'a Vec<PendingAck>);

impl Cluster {
    fn new(
        uri: StreamerUri,
        connect_options: Arc<RedisConnectOptions>,
        consumer_options: Arc<RedisConsumerOptions>,
        shards: Vec<ShardState>,
        messages: Sender<RedisResult<SharedMessage>>,
    ) -> RedisResult<Self> {
        if uri.nodes().is_empty() {
            return Err(StreamErr::StreamUrlErr(StreamUrlErr::ZeroNode));
        }
        Ok(Cluster {
            uri,
            connect_options,
            consumer_options,
            shards,
            messages,
            nodes: Default::default(),
            keys: Default::default(),
        })
    }

    async fn run(mut self, response: Receiver<CtrlMsg>) {
        let (sender, receiver) = bounded(128);
        let uri = self.uri.clone();
        for node_id in uri.nodes() {
            self.add_node(node_id.clone(), sender.clone());
        }
        {
            // we assign all shards to the first node, they will be moved later
            let first = uri.nodes().first().unwrap();
            let node = self.nodes.get(first).unwrap();
            for shard in std::mem::take(&mut self.shards) {
                self.keys.insert(shard.key().to_owned(), first.to_owned());
                node.send_async(CtrlMsg::AddShard(shard)).await.unwrap();
            }
        }
        'outer: loop {
            loop {
                match response.try_recv() {
                    Ok(res) => match res {
                        CtrlMsg::Ack(key, b, c) => {
                            if let Some(at) = self.keys.get(&key) {
                                let node = self.nodes.get(at).unwrap();
                                if node.send_async(CtrlMsg::Ack(key, b, c)).await.is_err() {
                                    break 'outer;
                                }
                            } else {
                                panic!("Unexpected shard `{:?}`", key);
                            }
                        }
                        CtrlMsg::AddShard(m) => panic!("Unexpected CtrlMsg {:?}", m),
                    },
                    Err(TryRecvError::Disconnected) => {
                        // Consumer is dead
                        break 'outer;
                    }
                    Err(TryRecvError::Empty) => break,
                }
            }
            // CAUTION `recv_async` is cancel safe as far as I understand
            if let Ok(Ok(event)) = timeout(ONE_SEC, receiver.recv_async()).await {
                match event {
                    ClusterEvent::Moved { shard, from, to } => {
                        log::info!("Shard {shard:?} moving from {from} to {to}");
                        self.add_node(to.clone(), sender.clone());
                        let node = self.nodes.get(&to).unwrap();
                        if let Some(key) = self.keys.get_mut(shard.key()) {
                            *key = to;
                        } else {
                            panic!("Unexpected shard `{}`", shard.key);
                        }
                        if node.send_async(CtrlMsg::AddShard(shard)).await.is_err() {
                            // node is dead
                            break;
                        }
                    }
                }
            }
        }
        log::debug!("Cluster {uri} exit");
    }

    fn add_node(&mut self, node_id: NodeId, event_sender: Sender<ClusterEvent>) {
        if self.nodes.get(&node_id).is_none() {
            let (ctrl_sender, receiver) = bounded(128);
            self.nodes.insert(node_id.clone(), ctrl_sender);
            let node = Node::new(
                node_id,
                self.connect_options.clone(),
                self.consumer_options.clone(),
                self.messages.clone(),
            );
            spawn_task(node.run(receiver, Some(event_sender)));
        }
    }
}

impl Node {
    fn new(
        id: NodeId,
        connect_options: Arc<RedisConnectOptions>,
        consumer_options: Arc<RedisConsumerOptions>,
        messages: Sender<RedisResult<SharedMessage>>,
    ) -> Self {
        let mut opts = StreamReadOptions::default()
            .count(BATCH_SIZE)
            .block(HEARTBEAT.as_secs() as usize * 1000);

        let mode = consumer_options.mode;
        let suffix = match mode {
            ConsumerMode::RealTime => "!",
            ConsumerMode::Resumable => "r",
            ConsumerMode::LoadBalanced => "s",
        };
        let group_id = if let Some(group_id) = consumer_options.consumer_group().ok() {
            group_id.name().to_owned()
        } else {
            format!("{}:{}", host_id(), suffix)
        };
        let consumer_id = match mode {
            ConsumerMode::RealTime | ConsumerMode::Resumable => group_id.clone(),
            ConsumerMode::LoadBalanced => format!("{}:{}", consumer_id(), suffix),
        };
        if matches!(mode, ConsumerMode::Resumable | ConsumerMode::LoadBalanced) {
            opts = opts.group(&group_id, &consumer_id);
            if consumer_options.auto_commit() == &AutoCommit::Immediate {
                opts = opts.noack();
            }
        }

        Self {
            id,
            connect_options,
            consumer_options,
            shards: Vec::new(),
            messages,
            opts,
            group: GroupState {
                group_id,
                first_read: true,
                pending_state: true,
            },
        }
    }

    async fn run(mut self, receiver: Receiver<CtrlMsg>, sender: Option<Sender<ClusterEvent>>) {
        let mut conn =
            Connection::create_or_reconnect(self.id.clone(), self.connect_options.clone())
                .await
                .unwrap();
        let mut ack_failure = 0;

        'outer: loop {
            loop {
                match receiver.try_recv() {
                    Ok(ctrl) => match ctrl {
                        CtrlMsg::AddShard(state) => {
                            log::debug!("Node {id} add shard {state:?}", id = self.id);
                            self.shards.push(state);
                            self.group.first_read = true;
                        }
                        CtrlMsg::Ack(key, id, ts) => {
                            self.ack_message(key, id, ts);
                        }
                    },
                    Err(TryRecvError::Disconnected) => {
                        // parent cluster is dead
                        break 'outer;
                    }
                    Err(TryRecvError::Empty) => break,
                }
            }
            if self.shards.is_empty() {
                sleep(ONE_SEC).await;
                continue;
            }
            let inner = match conn.get().await {
                Ok(inner) => inner,
                Err(StreamErr::Backend(RedisErr::TryAgain(_))) => continue, // it will sleep inside `get_connection`
                Err(err) => {
                    log::error!("{err}");
                    break;
                }
            };
            match self.read_next(inner).await {
                Ok(None) => (),
                Ok(Some(events)) => {
                    for event in events {
                        if let Some(sender) = &sender {
                            if sender.send_async(event).await.is_err() {
                                break 'outer;
                            }
                        } else {
                            panic!("Client is not configured to Redis Cluster");
                        }
                    }
                }
                Err(StreamErr::Backend(RedisErr::IoError(_))) => {
                    conn.reconnect();
                    continue;
                }
                Err(StreamErr::Backend(RedisErr::TryAgain(_))) => {
                    sleep(ONE_SEC).await;
                    continue;
                }
                Err(_) => {
                    break;
                }
            }
            match self.commit_ack(inner).await {
                Ok(_) => {
                    ack_failure = 0;
                }
                Err(StreamErr::Backend(RedisErr::IoError(_))) => {
                    conn.reconnect();
                    continue;
                }
                Err(err) => {
                    log::error!("{err}");
                    ack_failure += 1;
                    if ack_failure > 100 {
                        log::error!("Failed to ACK messages for too many times: {ack_failure}");
                        break;
                    }
                }
            }
        }

        log::debug!("Node {} exit", self.id);
    }

    async fn commit_ack(&mut self, conn: &mut redis::aio::Connection) -> RedisResult<()> {
        let mode = self.consumer_options.mode;
        if mode == ConsumerMode::RealTime {
            return Ok(());
        }
        if self.group.first_read {
            return Ok(());
        }
        for shard in self.shards.iter_mut() {
            if !shard.pending_ack.is_empty() {
                if self.consumer_options.auto_commit() == &AutoCommit::Disabled {
                    let to_ack = &shard.pending_ack;
                    log::debug!("XACK {} {} {}", shard.key, self.group.group_id, ad(to_ack));
                    match conn.xack(&shard.key, &self.group.group_id, to_ack).await {
                        Ok(()) => {
                            // success! so we clear our list
                            if self.consumer_options.auto_commit() == &AutoCommit::Disabled {
                                shard.pending_ack.truncate(0);
                            }
                        }
                        Err(err) => {
                            return Err(map_err(err));
                        }
                    }
                } else if self.consumer_options.auto_commit() == &AutoCommit::Delayed {
                    let mut to_ack = Vec::new();
                    let cut_off = Timestamp::now_utc() - *self.consumer_options.auto_commit_delay();
                    shard.pending_ack.retain(|ack| {
                        if ack.1 < cut_off {
                            to_ack.push(*ack);
                            false
                        } else {
                            true
                        }
                    });
                    if to_ack.is_empty() {
                        continue;
                    }
                    log::debug!("XACK {} {} {}", shard.key, self.group.group_id, ad(&to_ack));
                    match conn.xack(&shard.key, &self.group.group_id, &to_ack).await {
                        Ok(()) => (),
                        Err(err) => {
                            // error; we put back the items we have removed and try again later
                            to_ack.append(&mut shard.pending_ack);
                            shard.pending_ack = to_ack;
                            return Err(map_err(err));
                        }
                    }
                } else {
                    unreachable!()
                }
            }
        }

        fn ad<'a>(v: &'a Vec<PendingAck>) -> AckDisplay<'a> {
            AckDisplay(v)
        }

        Ok(())
    }

    async fn read_next(
        &mut self,
        conn: &mut redis::aio::Connection,
    ) -> RedisResult<Option<Vec<ClusterEvent>>> {
        let mode = self.consumer_options.mode;
        if mode == ConsumerMode::LoadBalanced {
            todo!()
        }
        if matches!(mode, ConsumerMode::Resumable | ConsumerMode::LoadBalanced) {
            if self.group.first_read {
                self.group.first_read = false;
                self.group.pending_state = true;
                for shard in self.shards.iter() {
                    let result: Result<Value, _> = conn
                        .xgroup_create(
                            &shard.key,
                            &self.group.group_id,
                            match self.consumer_options.auto_stream_reset() {
                                AutoStreamReset::Earliest => "0",
                                AutoStreamReset::Latest => "$",
                            },
                        )
                        .await;
                    match result {
                        Ok(_) => (),
                        Err(err) => {
                            if err.code() == Some("BUSYGROUP") {
                                // OK
                            } else {
                                return self.send_error(map_err(err)).await;
                            }
                        }
                    }
                }
            }
        }

        let mut cmd = command(match mode {
            ConsumerMode::RealTime => "XREAD",
            ConsumerMode::Resumable | ConsumerMode::LoadBalanced => "XREADGROUP",
        });
        cmd.arg(&self.opts).arg("STREAMS");

        for shard in self.shards.iter() {
            cmd.arg(&shard.key);
        }
        for shard in self.shards.iter() {
            match mode {
                ConsumerMode::RealTime => {
                    if let Some((a, b)) = shard.id {
                        cmd.arg(format!("{a}-{b}"));
                    } else {
                        cmd.arg(DOLLAR);
                    }
                }
                ConsumerMode::Resumable | ConsumerMode::LoadBalanced => {
                    if self.group.pending_state {
                        if let Some((a, b)) = shard.id {
                            cmd.arg(format!("{a}-{b}"));
                        } else {
                            cmd.arg("0-0");
                        }
                    } else {
                        cmd.arg(DIRECT);
                    }
                }
            }
        }

        match conn.req_packed_command(&cmd).await {
            Ok(value) => match StreamReadReply::from_redis_value(value) {
                Ok(res) => {
                    log::trace!("Node {} read {} messages", self.id, res.0.len());
                    if res.0.is_empty() {
                        // If we receive an empty reply, it means if we were reading the pending list
                        // then the list is now empty
                        self.group.pending_state = false;
                    }
                    for msg in res.0 {
                        let header = msg.header().to_owned();
                        if let Ok(()) = self.messages.send_async(Ok(msg)).await {
                            // we keep track of messages read ourselves
                            self.read_message(&header);
                        } else {
                            return Err(StreamErr::Backend(RedisErr::ConsumerDied));
                        }
                    }
                    Ok(None)
                }
                Err(err) => self.send_error(err).await,
            },
            Err(err) => {
                let kind = err.kind();
                if kind == ErrorKind::Moved {
                    // we don't know which key is moved, so we have to try all
                    let events = self.move_shards(conn).await;
                    Ok(Some(events))
                } else if kind == ErrorKind::IoError {
                    Err(StreamErr::Backend(RedisErr::IoError(err.to_string())))
                } else if matches!(
                    kind,
                    ErrorKind::Ask
                        | ErrorKind::TryAgain
                        | ErrorKind::ClusterDown
                        | ErrorKind::MasterDown
                ) {
                    // cluster is temporarily unavailable
                    Err(StreamErr::Backend(RedisErr::TryAgain(err.to_string())))
                } else {
                    self.send_error(map_err(err)).await
                }
            }
        }
    }

    async fn move_shards(&mut self, conn: &mut redis::aio::Connection) -> Vec<ClusterEvent> {
        let mut events = Vec::new();
        let shards = std::mem::take(&mut self.shards);
        for shard in shards {
            let result: Result<Value, _> = conn.xlen(&shard.key).await;
            match result {
                Ok(_) => {
                    // retain this shard
                    self.shards.push(shard);
                }
                Err(err) => {
                    if err.kind() == ErrorKind::Moved {
                        // remove this shard from self
                        events.push(ClusterEvent::Moved {
                            shard,
                            from: self.id.clone(),
                            to: match err.redirect_node() {
                                Some((to, _slot)) => {
                                    // `to` must be in form of `host:port` without protocol
                                    format!("{}://{}", self.id.scheme(), to)
                                        .parse()
                                        .expect("Failed to parse URL: {to}")
                                }
                                None => {
                                    panic!("Key is moved, but to where? {err:?}")
                                }
                            },
                        });
                    }
                }
            }
        }
        events
    }

    fn read_message(&mut self, header: &MessageHeader) {
        for shard in self.shards.iter_mut() {
            if shard.key == header.stream_key().name() {
                shard.update(header);
                return;
            }
        }
        panic!("Unknown shard {}", header.stream_key().name());
    }

    fn ack_message(&mut self, key: StreamShard, id: MessageId, ts: Timestamp) {
        for shard in self.shards.iter_mut() {
            if shard.key() == &key {
                shard.ack_message(id, ts);
                return;
            }
        }
        panic!("Unknown shard {:?}", key);
    }

    async fn send_error(&self, err: StreamErr<RedisErr>) -> RedisResult<Option<Vec<ClusterEvent>>> {
        if let StreamErr::Backend(err) = err {
            self.messages
                .send_async(Err(StreamErr::Backend(err.clone())))
                .await
                .ok();
            Err(StreamErr::Backend(err))
        } else {
            unreachable!()
        }
    }
}

impl ShardState {
    fn update(&mut self, header: &MessageHeader) {
        self.id = Some(get_message_id(header));
    }

    fn ack_message(&mut self, id: MessageId, ts: Timestamp) {
        self.pending_ack.push(PendingAck(id, ts));
    }

    fn key(&self) -> &StreamShard {
        &self.stream
    }
}

impl ToRedisArgs for PendingAck {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(format!("{}-{}", self.0 .0, self.0 .1).as_bytes())
    }
}

impl<'a> Display for AckDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, ack) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}-{}", ack.0 .0, ack.0 .1)?;
        }
        write!(f, "]")
    }
}

async fn discover_shards(
    cluster: &mut RedisCluster,
    stream: StreamKey,
) -> RedisResult<Vec<ShardState>> {
    let (_node, conn) = cluster.get_any()?;
    let shard_keys: Vec<String> = conn
        .keys(format!("{}:*", stream.name()))
        .await
        .map_err(map_err)?;

    Ok(if shard_keys.is_empty() {
        vec![ShardState {
            stream: (stream.clone(), ZERO),
            key: stream.name().to_owned(),
            id: None,
            pending_ack: Default::default(),
        }]
    } else {
        shard_keys
            .into_iter()
            .filter_map(|key| match key.split_once(':') {
                Some((_, tail)) => {
                    // make sure we can parse the tail
                    if let Ok(s) = tail.parse() {
                        Some(ShardState {
                            stream: (stream.clone(), ShardId::new(s)),
                            key,
                            id: None,
                            pending_ack: Default::default(),
                        })
                    } else {
                        log::warn!("Ignoring `{key}`");
                        None
                    }
                }
                None => unreachable!(),
            })
            .collect()
    })
}

pub fn consumer_id() -> String {
    format!(
        "{}:{}:{}",
        host_id(),
        std::process::id(),
        (Timestamp::now_utc().unix_timestamp_nanos() / 1_000_000) % 1_000_000
    )
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
