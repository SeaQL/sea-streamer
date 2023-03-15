use std::{collections::HashMap, sync::Arc, thread::ThreadId, time::Duration};

use flume::{
    bounded,
    r#async::{RecvFut, RecvStream},
    Receiver, RecvError, Sender, TryRecvError,
};
use redis::{
    aio::ConnectionLike, cmd as command, streams::StreamReadOptions, AsyncCommands, ErrorKind,
    Value,
};
use sea_streamer_runtime::{sleep, spawn_task, timeout};

use crate::{
    host_id, map_err, Connection, NodeId, RedisCluster, RedisConnectOptions, RedisErr, RedisResult,
};
use sea_streamer_types::{
    export::{
        async_trait,
        futures::{future::Map, FutureExt},
        url::Url,
    },
    Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions, Message, SeqPos, ShardId,
    SharedMessage, StreamErr, StreamKey, StreamUrlErr, StreamerUri, Timestamp,
};

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StreamReadReply(pub(crate) Vec<SharedMessage>);

#[derive(Debug)]
pub struct RedisConsumer {
    receiver: Receiver<RedisResult<SharedMessage>>,
    _handle: Sender<()>, // handle to drop the cluster
}

#[derive(Debug, Clone)]
pub struct RedisConsumerOptions {
    mode: ConsumerMode,
    group: Option<ConsumerGroup>,
    shared_shard: bool,
    consumer_timeout: Option<Duration>,
    auto_stream_reset: AutoStreamReset,
    enable_auto_commit: bool,
    auto_commit_interval: Duration,
}

pub const DEFAULT_AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(5);
pub const HEARTBEAT: Duration = Duration::from_secs(10);
pub const BATCH_SIZE: usize = 100;

const DOLLAR: &str = "$";
const ONE_SEC: Duration = Duration::from_secs(1);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AutoStreamReset {
    /// Use `0` as ID, which is the earliest message.
    Earliest,
    /// Use `$` as ID, which is the latest message.
    Latest,
}

// async fn xread_options<C, K, ID>(
//     con: &mut C,
//     keys: &[K],
//     ids: &[ID],
//     options: &StreamReadOptions,
// ) -> RedisResult<StreamReadReply>
// where
//     K: ToRedisArgs,
//     ID: ToRedisArgs,
//     C: ConnectionLike,
// {
//     let mut cmd = command(if options.read_only() {
//         "XREAD"
//     } else {
//         "XREADGROUP"
//     });
//     cmd.arg(options).arg("STREAMS").arg(keys).arg(ids);

//     let value = con.req_packed_command(&cmd).await.map_err(map_err)?;

//     StreamReadReply::from_redis_value(value)
// }

pub type NextFuture<'a> = Map<
    RecvFut<'a, RedisResult<SharedMessage>>,
    fn(Result<RedisResult<SharedMessage>, RecvError>) -> RedisResult<SharedMessage>,
>;

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
            enable_auto_commit: true,
            auto_commit_interval: DEFAULT_AUTO_COMMIT_INTERVAL,
        }
    }

    fn mode(&self) -> RedisResult<&ConsumerMode> {
        Ok(&self.mode)
    }

    /// ### Consumer ID
    ///
    /// Unlike Kafka, Redis requires consumers to self-assign consumer IDs.
    /// SeaStreamer uses a combination of `host id` + `process id` + `thread id` + `sub-second`.
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

    /// If enabled, read with `NOACK`. This acknowledges messages as soon as they are read.
    /// If you want to commit only what have been explicitly acked, set it to false.
    ///
    /// If unset, defaults to true.
    pub fn set_enable_auto_commit(&mut self, v: bool) -> &mut Self {
        self.enable_auto_commit = v;
        self
    }
    pub fn enable_auto_commit(&self) -> &bool {
        &self.enable_auto_commit
    }

    /// The interval for acks to be committed to the server.
    /// This option is only relevant when `enable_auto_commit` is false.
    ///
    /// If unset, defaults to [`DEFAULT_AUTO_COMMIT_INTERVAL`].
    pub fn set_auto_commit_interval(&mut self, v: Duration) -> &mut Self {
        self.auto_commit_interval = v;
        self
    }
    pub fn auto_commit_interval(&self) -> &Duration {
        &self.auto_commit_interval
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
        self.receiver.recv_async().map(|res| match res {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(StreamErr::Backend(RedisErr::ClientError(
                "Consumer died with unrecoverable error. Check the log for details.".to_owned(),
            ))),
        })
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        todo!()
    }
}

/// The caller should supply the thread id, which should be where `create_consumer` is called.
pub fn consumer_id_for(thread_id: ThreadId) -> String {
    let thread_id = format!("{:?}", thread_id);
    format!(
        "{}:{}:{}:{}",
        host_id(),
        std::process::id(),
        thread_id
            .trim_start_matches("ThreadId(")
            .trim_end_matches(')'),
        Timestamp::now_utc().millisecond()
    )
}

enum CtrlMsg {
    AddShard(ShardState),
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
    nodes: HashMap<Url, Sender<CtrlMsg>>,
}

struct Node {
    id: NodeId,
    connect_options: Arc<RedisConnectOptions>,
    consumer_options: Arc<RedisConsumerOptions>,
    shards: Vec<ShardState>,
    messages: Sender<RedisResult<SharedMessage>>,
}

#[derive(Debug)]
struct ShardState {
    key: String,
    id: Option<(u64, u16)>,
}

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
        })
    }

    async fn run(mut self, kill_switch: Receiver<()>) {
        let (sender, receiver) = bounded(128);
        let uri = self.uri.clone();
        for node_id in uri.nodes() {
            self.add_node(node_id.clone(), sender.clone());
        }
        {
            // we assign all shards to the first node, they will be moved later
            let node = self.nodes.get(uri.nodes().first().unwrap()).unwrap();
            for shard in std::mem::take(&mut self.shards) {
                node.send_async(CtrlMsg::AddShard(shard)).await.unwrap();
            }
        }
        loop {
            if let Err(TryRecvError::Disconnected) = kill_switch.try_recv() {
                break;
            }
            // CAUTION `recv_async` is cancel safe as far as I understand
            if let Ok(Ok(event)) = timeout(HEARTBEAT, receiver.recv_async()).await {
                match event {
                    ClusterEvent::Moved { shard, from, to } => {
                        log::info!("Shard {shard:?} moving from {from} to {to}");
                        self.add_node(to.clone(), sender.clone());
                        let node = self.nodes.get(&to).unwrap();
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
            let node = Node {
                id: node_id,
                connect_options: self.connect_options.clone(),
                consumer_options: self.consumer_options.clone(),
                shards: Vec::new(),
                messages: self.messages.clone(),
            };
            spawn_task(node.run(receiver, event_sender));
        }
    }
}

impl Node {
    async fn run(mut self, receiver: Receiver<CtrlMsg>, sender: Sender<ClusterEvent>) {
        let mut conn =
            Connection::create_or_reconnect(self.id.clone(), self.connect_options.clone())
                .await
                .unwrap();

        'outer: loop {
            loop {
                match receiver.try_recv() {
                    Ok(ctrl) => match ctrl {
                        CtrlMsg::AddShard(state) => {
                            log::debug!("Add shard {state:?}");
                            self.shards.push(state);
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
                Ok(None) => {}
                Ok(Some(events)) => {
                    for event in events {
                        if sender.send_async(event).await.is_err() {
                            break 'outer;
                        }
                    }
                }
                Err(StreamErr::Backend(RedisErr::IoError(_))) => {
                    conn.reconnect();
                }
                Err(StreamErr::Backend(RedisErr::TryAgain(_))) => {
                    sleep(ONE_SEC).await;
                }
                Err(_) => {
                    break;
                }
            }
        }

        log::debug!("Node {} exit", self.id);
    }

    async fn read_next(
        &mut self,
        conn: &mut redis::aio::Connection,
    ) -> RedisResult<Option<Vec<ClusterEvent>>> {
        let opts = StreamReadOptions::default()
            .count(BATCH_SIZE)
            .block(HEARTBEAT.as_secs() as usize * 1000);

        if self.consumer_options.mode == ConsumerMode::RealTime {
            let mut cmd = command("XREAD");
            cmd.arg(&opts).arg("STREAMS");

            for shard in self.shards.iter() {
                cmd.arg(&shard.key);
            }
            for shard in self.shards.iter() {
                if let Some((a, b)) = shard.id {
                    cmd.arg(format!("{a}-{b}"));
                } else {
                    cmd.arg(DOLLAR);
                }
            }

            match conn.req_packed_command(&cmd).await {
                Ok(value) => match StreamReadReply::from_redis_value(value) {
                    Ok(res) => {
                        log::trace!("read {} messages", res.0.len());
                        for msg in res.0 {
                            if let Ok(()) = self.messages.send_async(Ok(msg.clone())).await {
                                self.update(msg)
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
                    } else if matches!(kind, ErrorKind::ClusterDown | ErrorKind::MasterDown) {
                        // cluster is temporarily unavailable
                        Err(StreamErr::Backend(RedisErr::TryAgain(err.to_string())))
                    } else {
                        if matches!(kind, ErrorKind::Ask | ErrorKind::TryAgain) {
                            panic!("Impossible cluster error: {err}");
                        }
                        self.send_error(map_err(err)).await
                    }
                }
            }
        } else {
            todo!()
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

    fn update(&mut self, msg: SharedMessage) {
        for shard in self.shards.iter_mut() {
            if shard.key == msg.stream_key().name() {
                shard.update(&msg);
                return;
            }
        }
        panic!("Unknown shard {}", msg.stream_key().name());
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
    fn update(&mut self, msg: &SharedMessage) {
        self.id = Some((
            (msg.timestamp().unix_timestamp_nanos() / 1_000_000)
                .try_into()
                .expect("RedisConsumer: timestamp out of range"),
            (msg.sequence() & 0xFFFF).try_into().expect("Never fails"),
        ));
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
            key: stream.name().to_owned(),
            id: None,
        }]
    } else {
        shard_keys
            .into_iter()
            .filter_map(|key| match key.split_once(':') {
                Some((_, tail)) => {
                    // make sure we can parse the tail
                    if tail.parse::<u64>().is_ok() {
                        Some(ShardState { key, id: None })
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

pub(crate) async fn create_consumer(
    mut cluster: RedisCluster,
    consumer_options: RedisConsumerOptions,
    streams: Vec<StreamKey>,
) -> RedisResult<RedisConsumer> {
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
        Arc::new(consumer_options),
        shards,
        sender,
    )?;

    let (handle, kill_switch) = bounded(1);
    spawn_task(cluster.run(kill_switch));

    Ok(RedisConsumer {
        receiver,
        _handle: handle,
    })
}
