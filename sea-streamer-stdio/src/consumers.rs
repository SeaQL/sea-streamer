use flume::{unbounded, Receiver, Sender};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use sea_streamer::{
    MessageFrame, MessageMeta, SequenceNo, ShardId, StreamErr, StreamKey, StreamResult, Timestamp,
};

use crate::{
    parser::{parse_meta, PartialMeta},
    util::PanicGuard,
};

lazy_static::lazy_static! {
    static ref CONSUMERS: Mutex<Consumers> = Mutex::new(Default::default());
    static ref THREAD: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);
}

#[derive(Debug, Default)]
struct Consumers {
    consumers: HashMap<u64, ConsumerRelay>,
    sequences: HashMap<(StreamKey, ShardId), SequenceNo>,
}

#[derive(Debug)]
pub struct ConsumerRelay {
    streams: Vec<StreamKey>,
    sender: Sender<MessageFrame>,
}

pub struct Consumer {
    id: u64,
    receiver: Receiver<MessageFrame>,
}

impl Consumers {
    fn add(&mut self, streams: Vec<StreamKey>) -> Consumer {
        let (con, sender) = Consumer::new();
        assert!(
            self.consumers
                .insert(con.id, ConsumerRelay { streams, sender })
                .is_none(),
            "Duplicate consumer id"
        );
        con
    }

    fn remove(&mut self, id: u64) {
        assert!(
            self.consumers.remove(&id).is_some(),
            "Consumer with id {} does not exist",
            id
        );
    }

    /// this does not need to be mut, but we want to enforce a mutex lock
    pub(crate) fn dispatch(&mut self, meta: PartialMeta, bytes: Vec<u8>, offset: usize) {
        let stream_key = meta
            .stream_key
            .to_owned()
            .unwrap_or_else(|| StreamKey::new("broadcast".to_owned()));
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
        let message = MessageFrame::new(
            MessageMeta::new(
                stream_key,
                shard_id,
                sequence,
                meta.timestamp.unwrap_or_else(Timestamp::now_utc),
            ),
            bytes,
            offset,
        );
        for consumer in self.consumers.values() {
            if meta.stream_key.is_none()
                || consumer.streams.contains(meta.stream_key.as_ref().unwrap())
            {
                consumer.sender.send(message.clone()).ok();
            }
        }
    }
}

pub fn create_consumer(streams: Vec<StreamKey>) -> Consumer {
    init();
    let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
    consumers.add(streams)
}

pub(crate) fn init() {
    let mut thread = THREAD.lock().expect("Failed to lock thread");
    if thread.is_none() {
        let flag = Arc::new(AtomicBool::new(true));
        let local_flag = flag.clone();
        std::thread::spawn(move || {
            let _guard = PanicGuard;
            while local_flag.load(Ordering::Relaxed) {
                let mut line = String::new();
                match std::io::stdin().read_line(&mut line) {
                    Ok(0) => break, // this means stdin is closed
                    Ok(_) => {}
                    Err(e) => {
                        panic!("{:?}", e);
                    }
                }
                let (meta, remaining) =
                    parse_meta(&line).unwrap_or_else(|_| panic!("Failed to parse line: {}", line));
                let offset = remaining.as_ptr() as usize - line.as_ptr() as usize;
                dispatch(meta, line.into_bytes(), offset);
            }
        });
        thread.replace(flag);
    }
}

pub(crate) fn dispatch(meta: PartialMeta, bytes: Vec<u8>, offset: usize) {
    let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
    consumers.dispatch(meta, bytes, offset)
}

impl Consumer {
    fn new() -> (Self, Sender<MessageFrame>) {
        let (sender, receiver) = unbounded();
        (
            Self {
                id: fastrand::u64(..),
                receiver,
            },
            sender,
        )
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
        consumers.remove(self.id)
    }
}

impl Consumer {
    pub(crate) async fn next(&self) -> StreamResult<MessageFrame> {
        self.receiver
            .recv_async()
            .await
            .map_err(|e| StreamErr::Internal(Box::new(e)))
    }
}
