use flume::{
    r#async::{RecvFut, RecvStream},
    unbounded, Receiver, RecvError, Sender,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
};

use sea_streamer_types::{
    export::{
        async_trait,
        futures::{future::MapErr, stream::Map as StreamMap, StreamExt, TryFutureExt},
    },
    Consumer as ConsumerTrait, ConsumerGroup, Message, MessageHeader, SeqNo, SeqPos, ShardId,
    SharedMessage, StreamErr, StreamKey, Timestamp,
};

use crate::{
    parser::{parse_meta, PartialMeta},
    util::PanicGuard,
    StdioErr, StdioResult, BROADCAST,
};

lazy_static::lazy_static! {
    static ref CONSUMERS: Mutex<Consumers> = Mutex::new(Default::default());
    static ref THREAD: Mutex<bool> = Mutex::new(false);
}

type Cid = u64;

#[derive(Debug, Default)]
struct Consumers {
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

#[derive(Debug)]
pub struct StdioConsumer {
    id: Cid,
    receiver: Receiver<SharedMessage>,
}

pub type NextFuture<'a> = MapErr<RecvFut<'a, SharedMessage>, fn(RecvError) -> StreamErr<StdioErr>>;

pub type StdioMessageStream<'a> =
    StreamMap<RecvStream<'a, SharedMessage>, fn(SharedMessage) -> StdioResult<SharedMessage>>;

pub type StdioMessage = SharedMessage;

impl Consumers {
    fn add(&mut self, group: Option<ConsumerGroup>, streams: Vec<StreamKey>) -> StdioConsumer {
        let id = self.max_id;
        self.max_id += 1;
        let (con, sender) = StdioConsumer::new(id);
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

    fn remove(&mut self, id: Cid) {
        self.consumers.remove(&id);
    }

    fn dispatch(&mut self, meta: PartialMeta, bytes: Vec<u8>, offset: usize) {
        let stream_key = meta
            .stream_key
            .to_owned()
            .unwrap_or_else(|| StreamKey::new(BROADCAST).unwrap());
        let shard_id = meta.shard_id.unwrap_or_default();
        let entry = self
            .sequences
            .entry((stream_key.clone(), shard_id)) // unfortunate we have to clone
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

    fn disconnect(&mut self) {
        self.consumers = Default::default();
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
    let mut thread = THREAD.lock().expect("Failed to lock stdin thread");
    if !*thread {
        let builder = std::thread::Builder::new().name("sea-streamer-stdio-stdin".into());
        builder
            .spawn(move || {
                log::debug!("[{pid}] stdin thread spawned", pid = std::process::id());
                let _guard = PanicGuard;
                loop {
                    let mut line = String::new();
                    // this has the potential to block forever
                    match std::io::stdin().read_line(&mut line) {
                        Ok(0) => break, // this means stdin is closed
                        Ok(_) => {}
                        Err(e) => {
                            panic!("{e:?}");
                        }
                    }
                    if line.ends_with('\n') {
                        line.truncate(line.len() - 1);
                    }
                    let (meta, remaining) = parse_meta(&line)
                        .unwrap_or_else(|_| panic!("Failed to parse line: {line}"));
                    let offset = remaining.as_ptr() as usize - line.as_ptr() as usize;
                    dispatch(meta, line.into_bytes(), offset);
                }
                log::debug!("[{pid}] stdin thread exit", pid = std::process::id());
                {
                    let mut thread = THREAD.lock().expect("Failed to lock stdin thread");
                    *thread = false;
                }
            })
            .unwrap();
        *thread = true;
    }
}

pub(crate) fn disconnect() {
    let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
    consumers.disconnect()
}

pub(crate) fn dispatch(meta: PartialMeta, bytes: Vec<u8>, offset: usize) {
    let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
    consumers.dispatch(meta, bytes, offset)
}

impl StdioConsumer {
    fn new(id: Cid) -> (Self, Sender<SharedMessage>) {
        let (sender, receiver) = unbounded();
        (Self { id, receiver }, sender)
    }
}

impl Drop for StdioConsumer {
    fn drop(&mut self) {
        let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
        consumers.remove(self.id)
    }
}

#[async_trait]
impl ConsumerTrait for StdioConsumer {
    type Error = StdioErr;
    type Message<'a> = SharedMessage;
    // See we don't actually have to Box these! Looking forward to `type_alias_impl_trait`
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = StdioMessageStream<'a>;

    async fn seek(&mut self, _: Timestamp) -> StdioResult<()> {
        Err(StreamErr::Unsupported("StdioConsumer::seek".to_owned()))
    }

    fn rewind(&mut self, _: SeqPos) -> StdioResult<()> {
        Err(StreamErr::Unsupported("StdioConsumer::rewind".to_owned()))
    }

    // Always succeed
    fn assign(&mut self, _: ShardId) -> StdioResult<()> {
        Ok(())
    }

    fn next(&self) -> Self::NextFuture<'_> {
        self.receiver
            .recv_async()
            .map_err(|e| StreamErr::Backend(StdioErr::RecvError(e)))
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        self.receiver.stream().map(Result::Ok)
    }
}
