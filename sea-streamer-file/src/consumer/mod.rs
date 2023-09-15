mod future;
mod group;

pub use future::StreamFuture as FileMessageStream;

use flume::{r#async::RecvFut, Receiver, Sender, TrySendError};
use sea_streamer_types::{
    export::{
        async_trait,
        futures::{Future, FutureExt},
    },
    Consumer as ConsumerTrait, SeqPos, ShardId, SharedMessage, StreamErr, StreamKey, Timestamp,
};

use crate::{is_pulse, FileErr, FileId, FileResult, SeekTarget};
pub(crate) use group::new_consumer;
use group::Sid;

pub use self::group::query_streamer;
use self::group::{preseek_consumer, remove_consumer};

pub struct FileConsumer {
    file_id: FileId,
    sid: Sid,
    receiver: Receiver<Result<SharedMessage, FileErr>>,
    ctrl: Sender<CtrlMsg>,
}

impl std::fmt::Debug for FileConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileConsumer {{ sid: {} }}", self.sid)
    }
}

enum CtrlMsg {
    Read,
    Seek(SeekTarget),
}

pub enum NextFuture<'a> {
    Future(RecvFut<'a, Result<SharedMessage, FileErr>>),
    Error(Option<StreamErr<FileErr>>),
}

pub type FileMessage = SharedMessage;

impl FileConsumer {
    fn new(
        file_id: FileId,
        sid: Sid,
        receiver: Receiver<Result<SharedMessage, FileErr>>,
        ctrl: Sender<CtrlMsg>,
    ) -> Self {
        Self {
            file_id,
            sid,
            receiver,
            ctrl,
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
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = FileMessageStream<'a>;

    /// Affects all streams.
    /// If the consumer is subscribing to multiple streams, it will be sought by the first stream key.
    /// It revokes the group membership of the Consumer.
    async fn seek(&mut self, ts: Timestamp) -> FileResult<()> {
        self.seek_to(SeekTarget::Timestamp(ts))
            .await
            .map_err(StreamErr::Backend)
    }

    /// Affects all streams.
    /// If the consumer is subscribing to multiple streams, it will be sought by the first stream key.
    /// It revokes the group membership of the Consumer.
    async fn rewind(&mut self, to: SeqPos) -> FileResult<()> {
        self.seek_to(match to {
            SeqPos::Beginning => SeekTarget::Beginning,
            SeqPos::End => SeekTarget::End,
            SeqPos::At(at) => SeekTarget::SeqNo(at),
        })
        .await
        .map_err(StreamErr::Backend)
    }

    /// Currently unimplemented; always error.
    fn assign(&mut self, _: (StreamKey, ShardId)) -> FileResult<()> {
        Err(StreamErr::Unsupported(
            "Cannot manually assign shards".to_owned(),
        ))
    }

    /// Currently unimplemented; always error.
    fn unassign(&mut self, _: (StreamKey, ShardId)) -> FileResult<()> {
        Err(StreamErr::Unsupported(
            "Cannot manually assign shards".to_owned(),
        ))
    }

    /// If there is already a message in the buffer, it yields immediately.
    /// Otherwise it will await the next message.
    fn next(&self) -> Self::NextFuture<'_> {
        if let Err(TrySendError::Disconnected(_)) = self.ctrl.try_send(CtrlMsg::Read) {
            // race: there is a possibility that *after* we enter the receiver future
            // ctrl disconnect immediately. it will manifest in the StreamEnded below.
            NextFuture::Error(Some(StreamErr::Backend(FileErr::StreamEnded)))
        } else {
            NextFuture::Future(self.receiver.recv_async())
        }
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        FileMessageStream::new(self)
    }
}

impl FileConsumer {
    /// Seeking revokes the group membership of the Consumer
    ///
    /// Warning: This future must not be canceled.
    pub async fn seek_to(&mut self, target: SeekTarget) -> Result<(), FileErr> {
        // prepare the streamer
        preseek_consumer(&self.file_id, self.sid).await?;
        // send a request
        self.ctrl
            .send_async(CtrlMsg::Seek(target))
            .await
            .map_err(|_| FileErr::TaskDead("FileConsumer seek"))?;
        // drain until we get a pulse
        loop {
            match self.receiver.recv_async().await {
                Ok(Ok(msg)) => {
                    if is_pulse(&msg) {
                        break;
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(FileErr::TaskDead("FileConsumer seek")),
            }
        }
        Ok(())
    }
}

impl<'a> Future for NextFuture<'a> {
    type Output = FileResult<SharedMessage>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};
        match std::pin::Pin::into_inner(self) {
            Self::Error(e) => Ready(Err(e.take().unwrap())),
            Self::Future(future) => match future.poll_unpin(cx) {
                Ready(res) => match res {
                    Ok(Ok(m)) => Ready(Ok(m)),
                    Ok(Err(e)) => Ready(Err(StreamErr::Backend(e))),
                    Err(_) => Ready(Err(StreamErr::Backend(FileErr::StreamEnded))),
                },
                Pending => Pending,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn only_send_sync<C: ConsumerTrait + Send + Sync>(_: C) {}

    #[test]
    fn consumer_is_send_sync() {
        #[allow(dead_code)]
        fn ensure_send_sync(c: FileConsumer) {
            only_send_sync(c);
        }
    }
}
