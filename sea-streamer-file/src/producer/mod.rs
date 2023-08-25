mod backend;

use flume::{r#async::RecvFut, unbounded, Sender};
use std::{fmt::Debug, future::Future};

use crate::{Bytes, FileErr, FileId, FileResult};
use sea_streamer_types::{
    export::{async_trait, futures::FutureExt},
    Buffer, MessageHeader, Producer as ProducerTrait, ShardId, StreamErr, StreamKey, StreamResult,
    Timestamp,
};

pub(crate) use backend::{end_producer, new_producer};

#[derive(Debug)]
pub struct FileProducer {
    file_id: FileId,
    stream: Option<StreamKey>,
    master: &'static Sender<RequestTo>,
    sender: Sender<Request>,
}

pub struct SendFuture {
    fut: RecvFut<'static, Result<MessageHeader, FileErr>>,
}

#[derive(Debug)]
struct RequestTo {
    file_id: FileId,
    data: Request,
}

type Reply = Sender<Result<(), FileErr>>;

#[derive(Debug)]
enum Request {
    Send(SendRequest),
    Flush(Reply),
    End(Reply),
    Clone,
    Drop,
}

#[derive(Debug)]
struct SendRequest {
    stream_key: StreamKey,
    shard_id: ShardId,
    timestamp: Timestamp,
    bytes: Bytes,
    /// one shot
    receipt: Sender<Result<MessageHeader, FileErr>>,
}

impl Future for SendFuture {
    type Output = StreamResult<MessageHeader, FileErr>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.fut.poll_unpin(cx) {
            std::task::Poll::Ready(res) => std::task::Poll::Ready(match res {
                Ok(Ok(res)) => Ok(res),
                Ok(Err(e)) => Err(StreamErr::Backend(e)),
                Err(_) => Err(StreamErr::Backend(FileErr::ProducerEnded)),
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

    fn send_to<S: Buffer>(
        &self,
        stream_key: &StreamKey,
        buffer: S,
    ) -> FileResult<Self::SendFuture> {
        let err = || Err(StreamErr::Backend(FileErr::ProducerEnded));
        let (s, r) = unbounded();
        if self
            .sender
            .send(Request::Send(SendRequest {
                stream_key: stream_key.clone(),
                shard_id: ShardId::new(0),
                timestamp: Timestamp::now_utc(),
                bytes: Bytes::Bytes(buffer.into_bytes()),
                receipt: s,
            }))
            .is_err()
        {
            return err();
        }
        Ok(SendFuture {
            fut: r.into_recv_async(),
        })
    }

    async fn end(mut self) -> FileResult<()> {
        let err = || Err(StreamErr::Backend(FileErr::ProducerEnded));
        let (s, r) = unbounded();
        if self
            .master
            .send(RequestTo {
                file_id: self.file_id.clone(),
                data: Request::End(s),
            })
            .is_err()
        {
            return err();
        }
        match r.recv_async().await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(StreamErr::Backend(e)),
            Err(_) => err(),
        }
    }

    async fn flush(&mut self) -> FileResult<()> {
        let err = || Err(StreamErr::Backend(FileErr::ProducerEnded));
        let (s, r) = unbounded();
        if self.sender.send(Request::Flush(s)).is_err() {
            return err();
        }
        match r.recv_async().await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(StreamErr::Backend(e)),
            Err(_) => err(),
        }
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

impl Clone for FileProducer {
    fn clone(&self) -> Self {
        self.master
            .send(RequestTo {
                file_id: self.file_id.clone(),
                data: Request::Clone,
            })
            .expect("Request handler should never die");
        Self {
            file_id: self.file_id.clone(),
            stream: self.stream.clone(),
            master: self.master,
            sender: self.sender.clone(),
        }
    }
}

impl Drop for FileProducer {
    fn drop(&mut self) {
        self.master
            .send(RequestTo {
                file_id: self.file_id.clone(),
                data: Request::Drop,
            })
            .ok();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn only_send_sync<C: ProducerTrait + Send + Sync>(_: C) {}

    #[test]
    fn producer_is_send_sync() {
        #[allow(dead_code)]
        fn ensure_send_sync(p: FileProducer) {
            only_send_sync(p);
        }
    }
}
