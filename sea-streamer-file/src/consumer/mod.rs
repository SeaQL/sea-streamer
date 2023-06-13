mod future;
mod group;

pub use future::StreamFuture as FileMessageStream;

use flume::{r#async::SendFut, Receiver, Sender, TryRecvError};
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

pub struct NextFuture<'a> {
    inner: &'a FileConsumer,
    future: Option<SendFut<'a, CtrlMsg>>,
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

    /// If this future is canceled, a Message might still be read.
    /// It will be queued for you to consume on the next next() call,
    /// which yields immediately. In a nutshell, cancelling this
    /// future does not undo the read, but it will not cause any error.
    fn next(&self) -> Self::NextFuture<'_> {
        NextFuture {
            inner: self,
            future: None,
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

/// Hand-unrolled of the following:
/// ```ignore
/// loop {
///     if let Ok(m) = receiver.try_recv() {
///         return Ok(m);
///     }
///     ctrl.send_async(Read).await?;
/// }
/// ```
impl<'a> Future for NextFuture<'a> {
    type Output = FileResult<SharedMessage>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};
        loop {
            match self.inner.receiver.try_recv() {
                Ok(Ok(m)) => return Ready(Ok(m)),
                Ok(Err(e)) => return Ready(Err(StreamErr::Backend(e))),
                Err(TryRecvError::Disconnected) => {
                    return Ready(Err(StreamErr::Backend(FileErr::TaskDead("Streamer Recv"))))
                }
                Err(TryRecvError::Empty) => (),
            }
            if self.future.is_none() {
                self.future = Some(self.inner.ctrl.send_async(CtrlMsg::Read));
            }
            match self.future.as_mut().unwrap().poll_unpin(cx) {
                Ready(res) => match res {
                    Ok(_) => {
                        self.future = None;
                    }
                    Err(_) => return Ready(Err(StreamErr::Backend(FileErr::StreamEnded))),
                },
                Pending => return Pending,
            }
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
