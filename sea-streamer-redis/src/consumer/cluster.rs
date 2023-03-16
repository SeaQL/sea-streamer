use flume::{bounded, Receiver, Sender, TryRecvError};
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{Node, ShardState, StreamShard};
use crate::{Connection, MessageId, NodeId, RedisCluster, RedisConsumerOptions, RedisResult};
use sea_streamer_runtime::{sleep, spawn_task};
use sea_streamer_types::{SharedMessage, Timestamp};

const ONE_SEC: Duration = Duration::from_secs(1);

pub struct Cluster {
    shards: Vec<ShardState>,
    consumer_options: Arc<RedisConsumerOptions>,
    messages: Sender<RedisResult<SharedMessage>>,
    nodes: HashMap<NodeId, Sender<CtrlMsg>>,
    keys: HashMap<StreamShard, NodeId>,
}

#[allow(clippy::large_enum_variant)]
pub enum StatusMsg {
    Ready,
    Moved {
        shard: ShardState,
        from: NodeId,
        to: NodeId,
    },
}

// It's important to keep the messages small
pub enum CtrlMsg {
    Init(Box<(NodeId, Connection)>),
    AddShard(Box<ShardState>),
    Ack(StreamShard, MessageId, Timestamp),
}

impl Cluster {
    pub fn new(
        consumer_options: Arc<RedisConsumerOptions>,
        shards: Vec<ShardState>,
        messages: Sender<RedisResult<SharedMessage>>,
    ) -> RedisResult<Self> {
        Ok(Cluster {
            consumer_options,
            shards,
            messages,
            nodes: Default::default(),
            keys: Default::default(),
        })
    }

    pub async fn run(
        mut self,
        cluster: RedisCluster,
        response: Receiver<CtrlMsg>,
        status: Sender<StatusMsg>,
    ) {
        let RedisCluster {
            cluster: cluster_uri,
            options: connect_options,
            conn: connections,
            ..
        } = cluster;
        let (sender, receiver) = bounded(128);
        for (node_id, conn) in connections {
            let node = self.add_node(node_id.clone(), sender.clone());
            node.send_async(CtrlMsg::Init(Box::new((node_id, conn))))
                .await
                .unwrap();
        }
        {
            // we assign all shards to the first node, they will be moved later
            let (node_id, node) = self.nodes.iter().next().unwrap();
            for shard in std::mem::take(&mut self.shards) {
                self.keys.insert(shard.key().to_owned(), node_id.to_owned());
                node.send_async(CtrlMsg::AddShard(Box::new(shard)))
                    .await
                    .unwrap();
            }
        }
        let mut ready_count = 0;
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
                        _ => panic!("Unexpected CtrlMsg"),
                    },
                    Err(TryRecvError::Disconnected) => {
                        // Consumer is dead
                        break 'outer;
                    }
                    Err(TryRecvError::Empty) => break,
                }
            }

            if let Ok(event) = receiver.try_recv() {
                match event {
                    StatusMsg::Ready => {
                        ready_count += 1;
                        if ready_count == self.nodes.len() {
                            status.send_async(StatusMsg::Ready).await.ok();
                            log::debug!("Cluster {cluster_uri} ready");
                        }
                    }
                    StatusMsg::Moved { shard, from, to } => {
                        log::info!("Shard {shard:?} moving from {from} to {to}");
                        self.add_node(to.clone(), sender.clone());
                        let node = self.nodes.get(&to).unwrap();
                        if let Some(key) = self.keys.get_mut(shard.key()) {
                            *key = to.clone();
                        } else {
                            panic!("Unexpected shard `{}`", shard.key);
                        }
                        let conn =
                            Connection::create_or_reconnect(to.clone(), connect_options.clone())
                                .await
                                .unwrap();
                        node.send_async(CtrlMsg::Init(Box::new((to, conn))))
                            .await
                            .unwrap();
                        if node
                            .send_async(CtrlMsg::AddShard(Box::new(shard)))
                            .await
                            .is_err()
                        {
                            // node is dead
                            break;
                        }
                    }
                }
            }

            sleep(ONE_SEC).await;
        }
        log::debug!("Cluster {cluster_uri} exit");
    }

    fn add_node(&mut self, node_id: NodeId, event_sender: Sender<StatusMsg>) -> &Sender<CtrlMsg> {
        if self.nodes.get(&node_id).is_none() {
            let (ctrl_sender, receiver) = bounded(128);
            self.nodes.insert(node_id.clone(), ctrl_sender);
            let node = Node::add(
                node_id.to_owned(),
                self.consumer_options.clone(),
                self.messages.clone(),
            );
            spawn_task(node.run(receiver, event_sender));
        }
        self.nodes.get(&node_id).unwrap()
    }
}
