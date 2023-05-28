use flume::Sender;
use std::collections::{BTreeMap, HashMap};

use sea_streamer_types::{
    ConsumerGroup, Message, MessageHeader, SeqNo, ShardId, SharedMessage, StreamKey, Timestamp,
};

use crate::{ConsumerMember, PartialHeader, BROADCAST};

pub type Cid = u64;

#[derive(Debug, Default)]
pub struct Consumers {
    max_id: Cid,
    consumers: BTreeMap<Cid, ConsumerRelay>,
    sequences: HashMap<(StreamKey, ShardId), SeqNo>,
}

/// We use flume because it works on any async runtime. But actually we only wanted a SPSC queue.
#[derive(Debug)]
struct ConsumerRelay {
    group: Option<ConsumerGroup>,
    streams: Vec<StreamKey>,
    sender: Sender<SharedMessage>,
}

impl Consumers {
    pub fn add(&mut self, group: Option<ConsumerGroup>, streams: Vec<StreamKey>) -> ConsumerMember {
        let id = self.max_id;
        self.max_id += 1;
        let (con, sender) = ConsumerMember::new(id, streams.clone());
        self.consumers.insert(
            id,
            ConsumerRelay {
                group,
                streams,
                sender,
            },
        );
        con
    }

    pub fn remove(&mut self, id: Cid) {
        self.consumers.remove(&id);
    }

    pub fn dispatch(&mut self, meta: PartialHeader, bytes: Vec<u8>, offset: usize) {
        let stream_key = meta
            .stream_key
            .clone()
            .unwrap_or_else(|| StreamKey::new(BROADCAST).unwrap());
        let shard_id = meta.shard_id.unwrap_or_default();
        let entry = self
            .sequences
            .entry((stream_key.clone(), shard_id))
            .or_default();
        let sequence = if let Some(sequence) = meta.sequence {
            *entry = sequence;
            sequence
        } else {
            let ret = *entry;
            *entry = ret + 1;
            ret
        };
        let length = bytes.len() - offset;
        let message = SharedMessage::new(
            MessageHeader::new(
                stream_key,
                shard_id,
                sequence,
                meta.timestamp.unwrap_or_else(Timestamp::now_utc),
            ),
            bytes,
            offset,
            length,
        );

        // We construct group membership on-the-fly so that consumers can join/leave a group anytime
        let mut groups: BTreeMap<ConsumerGroup, Vec<Cid>> = Default::default();

        for (cid, consumer) in self.consumers.iter() {
            if meta.stream_key.is_none() // broadcast message
                || consumer.streams.contains(meta.stream_key.as_ref().unwrap())
            {
                match &consumer.group {
                    Some(group) => {
                        if let Some(vec) = groups.get_mut(group) {
                            vec.push(*cid);
                        } else {
                            groups.insert(group.to_owned(), vec![*cid]);
                        }
                    }
                    None => {
                        // we don't care if it cannot be delivered
                        consumer.sender.send(message.clone()).ok();
                    }
                }
            }
        }

        for ids in groups.values() {
            // This round-robin is deterministic
            let id = ids[message.sequence() as usize % ids.len()];
            let consumer = self.consumers.get(&id).unwrap();
            // ignore any error
            consumer.sender.send(message.clone()).ok();
        }
    }

    pub fn disconnect(&mut self) {
        self.consumers = Default::default();
    }
}
