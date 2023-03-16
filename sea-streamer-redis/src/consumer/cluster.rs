use flume::{bounded, Receiver, Sender, TryRecvError};
use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{Node, ShardState, StreamShard};
use crate::{MessageId, NodeId, RedisConnectOptions, RedisConsumerOptions, RedisResult};
use sea_streamer_runtime::{sleep, spawn_task};
use sea_streamer_types::{SharedMessage, StreamErr, StreamUrlErr, StreamerUri, Timestamp};

const ONE_SEC: Duration = Duration::from_secs(1);

pub struct Cluster {
    uri: StreamerUri,
    connect_options: Arc<RedisConnectOptions>,
    shards: Vec<ShardState>,
    consumer_options: Arc<RedisConsumerOptions>,
    messages: Sender<RedisResult<SharedMessage>>,
    nodes: HashMap<NodeId, Sender<CtrlMsg>>,
    keys: HashMap<StreamShard, NodeId>,
}

pub enum ClusterEvent {
    Moved {
        shard: ShardState,
        from: NodeId,
        to: NodeId,
    },
}

pub enum CtrlMsg {
    AddShard(ShardState),
    Ack(StreamShard, MessageId, Timestamp),
}

impl Cluster {
    pub fn new(
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

    pub async fn run(mut self, response: Receiver<CtrlMsg>) {
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

            if let Ok(event) = receiver.try_recv() {
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

            sleep(ONE_SEC).await;
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
