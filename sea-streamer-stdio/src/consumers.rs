use async_trait::async_trait;
use flume::{unbounded, Receiver, Sender};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use sea_streamer::{
    export::futures::Stream, Consumer as ConsumerTrait, ConsumerGroup, Message, MessageMeta,
    SequenceNo, ShardId, StreamErr, StreamKey, StreamResult, Timestamp,
};

use crate::{
    parser::{parse_meta, PartialMeta},
    util::PanicGuard,
};

lazy_static::lazy_static! {
    static ref CONSUMERS: Mutex<Consumers> = Mutex::new(Default::default());
    static ref THREAD: Mutex<Option<Arc<AtomicBool>>> = Mutex::new(None);
}

type Cid = u64;

#[derive(Debug, Default)]
struct Consumers {
    consumers: HashMap<Cid, ConsumerRelay>,
    sequences: HashMap<(StreamKey, ShardId), SequenceNo>,
}

#[derive(Debug)]
struct ConsumerRelay {
    group: Option<ConsumerGroup>,
    streams: Vec<StreamKey>,
    sender: Sender<Message>,
}

#[derive(Debug)]
pub struct StdioConsumer {
    id: Cid,
    receiver: Receiver<Message>,
}

impl Consumers {
    fn add(&mut self, group: Option<ConsumerGroup>, streams: Vec<StreamKey>) -> StdioConsumer {
        let (con, sender) = StdioConsumer::new();
        assert!(
            self.consumers
                .insert(
                    con.id,
                    ConsumerRelay {
                        group,
                        streams,
                        sender
                    }
                )
                .is_none(),
            "Duplicate consumer id"
        );
        con
    }

    fn remove(&mut self, id: u64) {
        assert!(
            self.consumers.remove(&id).is_some(),
            "StdioConsumer with id {} does not exist",
            id
        );
    }

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
        let message = Message::new(
            MessageMeta::new(
                stream_key,
                shard_id,
                sequence,
                meta.timestamp.unwrap_or_else(Timestamp::now_utc),
            ),
            bytes,
            offset,
        );

        let mut groups: HashMap<ConsumerGroup, Vec<Cid>> = Default::default();
        for (cid, consumer) in self.consumers.iter() {
            if meta.stream_key.is_none()
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
                        consumer.sender.send(message.clone()).ok();
                    }
                }
            }
        }

        for ids in groups.values() {
            let id = ids[message.sequence() as usize % ids.len()];
            let consumer = self.consumers.get(&id).unwrap();
            consumer.sender.send(message.clone()).ok();
        }
    }
}

pub(crate) fn create_consumer(
    group: Option<ConsumerGroup>,
    streams: Vec<StreamKey>,
) -> StdioConsumer {
    init();
    let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
    consumers.add(group, streams)
}

pub(crate) fn init() {
    let mut thread = THREAD.lock().expect("Failed to lock thread");
    if thread.is_none() {
        let flag = Arc::new(AtomicBool::new(true));
        let local_flag = flag.clone();
        std::thread::spawn(move || {
            log::info!("stdin thread spawned");
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
            log::info!("stdin thread exit");
        });
        thread.replace(flag);
    }
}

pub(crate) fn dispatch(meta: PartialMeta, bytes: Vec<u8>, offset: usize) {
    let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
    consumers.dispatch(meta, bytes, offset)
}

impl StdioConsumer {
    fn new() -> (Self, Sender<Message>) {
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

impl Drop for StdioConsumer {
    fn drop(&mut self) {
        let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
        consumers.remove(self.id)
    }
}

impl StdioConsumer {
    pub(crate) async fn next(&self) -> StreamResult<Message> {
        self.receiver
            .recv_async()
            .await
            .map_err(|e| StreamErr::Internal(Box::new(e)))
    }
}

#[async_trait]
impl ConsumerTrait for StdioConsumer {
    type Stream = Pin<Box<dyn Stream<Item = StreamResult<Message>>>>;

    fn seek(&self, _: Timestamp) -> StreamResult<()> {
        Err(StreamErr::Unsupported("StdioConsumer::seek".to_owned()))
    }

    fn rewind(&self, _: SequenceNo) -> StreamResult<()> {
        Err(StreamErr::Unsupported("StdioConsumer::rewind".to_owned()))
    }

    fn assign(&self, _: ShardId) -> StreamResult<()> {
        Err(StreamErr::Unsupported("StdioConsumer::assign".to_owned()))
    }

    async fn next(&self) -> StreamResult<Message> {
        self.next().await
    }

    fn stream(self) -> Self::Stream {
        Box::pin(async_stream::try_stream! {
            loop {
                let mess = self.next().await?;
                yield mess;
            }
        })
    }
}
