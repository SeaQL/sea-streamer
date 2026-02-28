use flume::{
    Receiver, RecvError, Sender,
    r#async::{RecvFut, RecvStream},
    unbounded,
};
use std::sync::Mutex;

use sea_streamer_types::{
    Consumer as ConsumerTrait, ConsumerGroup, SeqPos, ShardId, SharedMessage, StreamErr, StreamKey,
    Timestamp,
    export::futures::{StreamExt, TryFutureExt, future::MapErr, stream::Map as StreamMap},
};

use crate::{
    PartialHeader, StdioErr, StdioResult,
    consumer_group::{Cid, Consumers},
    parse_meta,
    util::PanicGuard,
};

static CONSUMERS: std::sync::LazyLock<Mutex<Consumers>> =
    std::sync::LazyLock::new(|| Mutex::new(Default::default()));
static THREAD: std::sync::LazyLock<Mutex<bool>> = std::sync::LazyLock::new(|| Mutex::new(false));

#[derive(Debug)]
pub struct StdioConsumer {
    id: Cid,
    streams: Vec<StreamKey>,
    receiver: Receiver<SharedMessage>,
}

pub(crate) type ConsumerMember = StdioConsumer;

pub type NextFuture<'a> = MapErr<RecvFut<'a, SharedMessage>, fn(RecvError) -> StreamErr<StdioErr>>;

pub type StdioMessageStream<'a> =
    StreamMap<RecvStream<'a, SharedMessage>, fn(SharedMessage) -> StdioResult<SharedMessage>>;

pub type StdioMessage = SharedMessage;

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

pub(crate) fn dispatch(meta: PartialHeader, bytes: Vec<u8>, offset: usize) {
    let mut consumers = CONSUMERS.lock().expect("Failed to lock Consumers");
    consumers.dispatch(meta, bytes, offset)
}

impl StdioConsumer {
    pub(crate) fn new(id: Cid, streams: Vec<StreamKey>) -> (Self, Sender<SharedMessage>) {
        let (sender, receiver) = unbounded();
        (
            Self {
                id,
                streams,
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

impl ConsumerTrait for StdioConsumer {
    type Error = StdioErr;
    type Message<'a> = SharedMessage;
    // See we don't actually have to Box these! Looking forward to `type_alias_impl_trait`
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = StdioMessageStream<'a>;

    async fn seek(&mut self, _: Timestamp) -> StdioResult<()> {
        Err(StreamErr::Unsupported("StdioConsumer::seek".to_owned()))
    }

    async fn rewind(&mut self, _: SeqPos) -> StdioResult<()> {
        Err(StreamErr::Unsupported("StdioConsumer::rewind".to_owned()))
    }

    /// Always succeed if the stream exists. There is only shard ZERO anyway.
    fn assign(&mut self, (s, _): (StreamKey, ShardId)) -> StdioResult<()> {
        for stream in self.streams.iter() {
            if &s == stream {
                return Ok(());
            }
        }
        Err(StreamErr::StreamKeyNotFound)
    }

    /// Always fail. There is only shard ZERO anyway.
    fn unassign(&mut self, _: (StreamKey, ShardId)) -> StdioResult<()> {
        Err(StreamErr::StreamKeyNotFound)
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
