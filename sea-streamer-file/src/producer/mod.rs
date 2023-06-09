mod backend;

use flume::{r#async::RecvFut, Sender};
use std::{fmt::Debug, future::Future};

use crate::{Bytes, FileErr, FileId, FileResult};
use sea_streamer_types::{
    export::{async_trait, futures::FutureExt},
    Buffer, MessageHeader, Producer as ProducerTrait, Receipt, ShardId, StreamErr, StreamKey,
    StreamResult, Timestamp,
};

#[derive(Debug, Clone)]
pub struct FileProducer {
    file_id: FileId,
    stream: Option<StreamKey>,
    sender: &'static Sender<RequestTo>,
}

pub struct SendFuture {
    fut: RecvFut<'static, Receipt>,
}

struct RequestTo {
    file_id: FileId,
    data: Request,
}

enum Request {
    Send(SendRequest),
    End,
}

struct SendRequest {
    stream_key: StreamKey,
    shard_id: ShardId,
    timestamp: Timestamp,
    bytes: Bytes,
    /// one shot
    receipt: Sender<Result<Receipt, FileErr>>,
}

impl Future for SendFuture {
    type Output = StreamResult<MessageHeader, FileErr>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.fut.poll_unpin(cx) {
            std::task::Poll::Ready(res) => std::task::Poll::Ready(match res {
                Ok(res) => Ok(res),
                Err(err) => Err(StreamErr::Backend(FileErr::RecvError(err))),
            }),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Debug for SendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFuture").finish()
    }
}

#[async_trait]
impl ProducerTrait for FileProducer {
    type Error = FileErr;
    type SendFuture = SendFuture;

    fn send_to<S: Buffer>(&self, _: &StreamKey, _: S) -> FileResult<Self::SendFuture> {
        todo!();
    }

    #[inline]
    async fn end(mut self) -> FileResult<()> {
        self.flush().await
    }

    #[inline]
    async fn flush(&mut self) -> FileResult<()> {
        todo!();
    }

    fn anchor(&mut self, stream: StreamKey) -> FileResult<()> {
        if self.stream.is_none() {
            self.stream = Some(stream);
            Ok(())
        } else {
            Err(StreamErr::AlreadyAnchored)
        }
    }

    fn anchored(&self) -> FileResult<&StreamKey> {
        if let Some(stream) = &self.stream {
            Ok(stream)
        } else {
            Err(StreamErr::NotAnchored)
        }
    }
}
