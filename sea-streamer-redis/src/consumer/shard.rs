use super::PendingAck;
use crate::{MessageId, RedisCluster, RedisResult, ZERO, get_message_id, map_err};
use redis::AsyncCommands;
use sea_streamer_types::{MessageHeader, ShardId, StreamKey, Timestamp};

pub type StreamShard = (StreamKey, ShardId);

#[derive(Debug)]
pub struct ShardState {
    pub stream: StreamShard,
    pub key: String,
    pub id: Option<MessageId>,
    pub pending_ack: Vec<PendingAck>,
}

impl ShardState {
    pub fn update(&mut self, header: &MessageHeader) {
        self.id = Some(get_message_id(header));
    }

    pub fn ack_message(&mut self, id: MessageId, ts: Timestamp) {
        self.pending_ack.push(PendingAck(id, ts));
    }

    pub fn key(&self) -> &StreamShard {
        &self.stream
    }
}

pub fn format_stream_shard((a, b): &StreamShard) -> String {
    format!("{a}:{b}")
}

pub async fn discover_shards(
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
        let mut shards: Vec<_> = shard_keys
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
            .collect();
        shards.sort_by_key(|s| s.stream.1);
        shards
    })
}
