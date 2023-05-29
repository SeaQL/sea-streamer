mod group;

use flume::{
    r#async::{RecvFut, RecvStream},
    Receiver, RecvError, Sender,
};
use sea_streamer_types::{
    export::{
        async_trait,
        futures::{future::Map as FutureMap, FutureExt},
    },
    Consumer as ConsumerTrait, SeqPos, ShardId, SharedMessage, StreamErr, StreamKey, Timestamp,
};

use crate::{FileErr, FileResult};
pub(crate) use group::new_consumer;
use group::{Pulse, Sid};

use self::group::remove_consumer;

pub struct FileConsumer {
    sid: Sid,
    receiver: Receiver<Result<SharedMessage, FileErr>>,
    pacer: Sender<Pulse>,
}

type ReceiveResult = Result<Result<SharedMessage, FileErr>, RecvError>;
pub type NextFuture<'a> = FutureMap<
    RecvFut<'a, Result<SharedMessage, FileErr>>,
    fn(ReceiveResult) -> FileResult<SharedMessage>,
>;

pub type FileMessageStream<'a> = RecvStream<'a, FileResult<SharedMessage>>;

pub type FileMessage = SharedMessage;

impl FileConsumer {
    pub(crate) fn new(
        sid: Sid,
        receiver: Receiver<Result<SharedMessage, FileErr>>,
        pacer: Sender<Pulse>,
    ) -> Self {
        Self {
            sid,
            receiver,
            pacer,
        }
    }
}

impl Drop for FileConsumer {
    fn drop(&mut self) {
        remove_consumer(self.sid);
    }
}

#[async_trait]
impl ConsumerTrait for FileConsumer {
    type Error = FileErr;
    type Message<'a> = SharedMessage;
    // See we don't actually have to Box these! Looking forward to `type_alias_impl_trait`
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = FileMessageStream<'a>;

    async fn seek(&mut self, _: Timestamp) -> FileResult<()> {
        Err(StreamErr::Unsupported("FileConsumer::seek".to_owned()))
    }

    async fn rewind(&mut self, _: SeqPos) -> FileResult<()> {
        Err(StreamErr::Unsupported("FileConsumer::rewind".to_owned()))
    }

    fn assign(&mut self, _: (StreamKey, ShardId)) -> FileResult<()> {
        todo!()
    }

    fn unassign(&mut self, _: (StreamKey, ShardId)) -> FileResult<()> {
        todo!()
    }

    fn next(&self) -> Self::NextFuture<'_> {
        if self.receiver.is_empty() {
            self.pacer.try_send(Pulse).ok();
        }
        self.receiver.recv_async().map(map_res)
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        todo!()
    }
}

fn map_res(res: ReceiveResult) -> FileResult<SharedMessage> {
    match res {
        Ok(Ok(m)) => Ok(m),
        Ok(Err(e)) => Err(StreamErr::Backend(e)),
        Err(e) => Err(StreamErr::Backend(FileErr::RecvError(e))),
    }
}
