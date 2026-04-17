use sea_streamer_types::{Message, Payload, SeqNo, ShardId, StreamKey, Timestamp};

#[derive(Debug, Clone)]
pub struct IggyMessage {
    stream_key: StreamKey,
    shard_id: ShardId,
    sequence: SeqNo,
    timestamp: Timestamp,
    payload: Vec<u8>,
}

impl IggyMessage {
    pub fn new(
        stream_key: StreamKey,
        shard_id: ShardId,
        sequence: SeqNo,
        timestamp: Timestamp,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            stream_key,
            shard_id,
            sequence,
            timestamp,
            payload,
        }
    }
}

impl Message for IggyMessage {
    fn stream_key(&self) -> StreamKey {
        self.stream_key.clone()
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    fn sequence(&self) -> SeqNo {
        self.sequence
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn message(&self) -> Payload<'_> {
        Payload::new(self.payload.as_slice())
    }
}
