use flume::{Receiver, Sender, TryRecvError};
use redis::{
    aio::ConnectionLike, cmd as command, streams::StreamReadOptions, AsyncCommands, ErrorKind,
    RedisWrite, ToRedisArgs, Value,
};
use std::{fmt::Display, sync::Arc, time::Duration};

use super::{
    AutoCommit, AutoStreamReset, CtrlMsg, ShardState, StatusMsg, StreamShard, BATCH_SIZE, HEARTBEAT,
};
use crate::{
    host_id, map_err, MessageId, NodeId, RedisCluster, RedisConsumerOptions, RedisErr, RedisResult,
    StreamReadReply,
};
use sea_streamer_runtime::sleep;
use sea_streamer_types::{
    ConsumerMode, ConsumerOptions, MessageHeader, SharedMessage, StreamErr, Timestamp,
};

const DOLLAR: &str = "$";
const DIRECT: &str = ">";
const ONE_SEC: Duration = Duration::from_secs(1);

pub struct Node {
    id: NodeId,
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

#[derive(Debug, Clone, Copy)]
pub struct PendingAck(pub MessageId, pub Timestamp);

#[repr(transparent)]
struct AckDisplay<'a>(&'a Vec<PendingAck>);

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

impl Node {
    /// Create a standalone node, without parent cluster
    pub fn new(
        cluster: RedisCluster,
        options: Arc<RedisConsumerOptions>,
        shards: Vec<ShardState>,
        handle: Sender<CtrlMsg>,
        messages: Sender<RedisResult<SharedMessage>>,
    ) -> RedisResult<Self> {
        let (node_id, conn) = cluster.conn.into_iter().next().unwrap();
        let node = Node::add(node_id.clone(), options, messages);
        handle
            .send(CtrlMsg::Init(Box::new((node_id, conn))))
            .unwrap();
        for shard in shards {
            handle.send(CtrlMsg::AddShard(Box::new(shard))).unwrap();
        }
        Ok(node)
    }

    /// Create a node that is managed by a cluster
    pub fn add(
        id: NodeId,
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
        let group_id = if let Ok(group_id) = consumer_options.consumer_group() {
            group_id.name().to_owned()
        } else {
            format!("{}:{}", host_id(), suffix)
        };
        let consumer_id = match mode {
            ConsumerMode::RealTime | ConsumerMode::Resumable => group_id.clone(),
            ConsumerMode::LoadBalanced => format!("{}:{}", consumer_id(), suffix),
        };
        if matches!(mode, ConsumerMode::Resumable | ConsumerMode::LoadBalanced) {
            opts = opts.group(&group_id, consumer_id);
            if consumer_options.auto_commit() == &AutoCommit::Immediate {
                opts = opts.noack();
            }
        }

        Self {
            id,
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

    pub async fn run(mut self, receiver: Receiver<CtrlMsg>, sender: Sender<StatusMsg>) {
        let mut conn = match receiver.recv_async().await {
            Ok(ctrl) => match ctrl {
                CtrlMsg::Init(boxed) => {
                    let (node_id, conn) = unbox(boxed);
                    if node_id == self.id {
                        conn
                    } else {
                        panic!("Not {}?", self.id);
                    }
                }
                _ => panic!("Unexpected CtrlMsg"),
            },
            Err(_) => {
                log::error!("Cluster dead?");
                return;
            }
        };
        let mut ack_failure = 0;
        let mut ready = false;

        'outer: loop {
            loop {
                match receiver.try_recv() {
                    Ok(ctrl) => match ctrl {
                        CtrlMsg::Init(_) => panic!("Unexpected CtrlMsg"),
                        CtrlMsg::AddShard(state) => {
                            log::debug!("Node {id} add shard {state:?}", id = self.id);
                            self.shards.push(unbox(state));
                            self.group.first_read = true;
                        }
                        CtrlMsg::Ack(key, id, ts) => {
                            self.ack_message(key, id, ts);
                        }
                        CtrlMsg::Kill(signal) => {
                            signal.send(()).ok();
                            break 'outer;
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
            if !ready {
                ready = true;
                if sender.send_async(StatusMsg::Ready).await.is_err() {
                    break 'outer;
                }
            }
            match self.read_next(inner).await {
                Ok(None) => (),
                Ok(Some(events)) => {
                    for event in events {
                        if sender.send_async(event).await.is_err() {
                            break 'outer;
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
        if self.consumer_options.auto_commit() == &AutoCommit::Immediate {
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

        fn ad(v: &Vec<PendingAck>) -> AckDisplay {
            AckDisplay(v)
        }

        Ok(())
    }

    async fn read_next(
        &mut self,
        conn: &mut redis::aio::Connection,
    ) -> RedisResult<Option<Vec<StatusMsg>>> {
        let mode = self.consumer_options.mode;
        if mode == ConsumerMode::LoadBalanced {
            todo!()
        }
        if matches!(mode, ConsumerMode::Resumable | ConsumerMode::LoadBalanced)
            && self.group.first_read
        {
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
                        match self.consumer_options.auto_stream_reset() {
                            AutoStreamReset::Earliest => cmd.arg("0-0"),
                            AutoStreamReset::Latest => cmd.arg(DOLLAR),
                        };
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

        if false {
            log::debug!(
                "{}",
                std::str::from_utf8(cmd.get_packed_command().as_slice()).unwrap()
            );
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

    async fn move_shards(&mut self, conn: &mut redis::aio::Connection) -> Vec<StatusMsg> {
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
                        events.push(StatusMsg::Moved {
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

    async fn send_error(&self, err: StreamErr<RedisErr>) -> RedisResult<Option<Vec<StatusMsg>>> {
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

pub fn consumer_id() -> String {
    format!(
        "{}:{}:{}",
        host_id(),
        std::process::id(),
        (Timestamp::now_utc().unix_timestamp_nanos() / 1_000_000) % 1_000_000
    )
}

#[allow(clippy::boxed_local)]
fn unbox<T>(value: Box<T>) -> T {
    *value
}
