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

use crate::{FileErr, FileResult};
pub(crate) use group::new_consumer;
use group::{Pulse, Sid};

use self::group::remove_consumer;

pub struct FileConsumer {
    sid: Sid,
    receiver: Receiver<Result<SharedMessage, FileErr>>,
    pulse: Sender<Pulse>,
}

pub struct NextFuture<'a> {
    inner: &'a FileConsumer,
    future: Option<SendFut<'a, Pulse>>,
}

pub type FileMessage = SharedMessage;

impl FileConsumer {
    pub(crate) fn new(
        sid: Sid,
        receiver: Receiver<Result<SharedMessage, FileErr>>,
        pulse: Sender<Pulse>,
    ) -> Self {
        Self {
            sid,
            receiver,
            pulse,
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

    async fn seek(&mut self, _: Timestamp) -> FileResult<()> {
        todo!()
    }

    async fn rewind(&mut self, _: SeqPos) -> FileResult<()> {
        todo!()
    }

    fn assign(&mut self, _: (StreamKey, ShardId)) -> FileResult<()> {
        todo!()
    }

    fn unassign(&mut self, _: (StreamKey, ShardId)) -> FileResult<()> {
        todo!()
    }

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

/// Hand-unrolled of the following:
/// ```ignore
/// loop {
///     if let Ok(m) = receiver.try_recv() {
///         return Ok(m);
///     }
///     pulse.send_async().await?;
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
                self.future = Some(self.inner.pulse.send_async(Pulse));
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
