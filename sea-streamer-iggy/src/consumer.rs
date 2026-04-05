use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use sea_streamer_types::{
    Consumer, SeqPos, ShardId, StreamErr, StreamKey, StreamResult, Timestamp,
    export::futures::Stream,
};

use crate::error::IggyErr;
use crate::message::IggyMessage;

pub struct IggyConsumer {
    receiver: flume::Receiver<StreamResult<IggyMessage, IggyErr>>,
    _handle: Arc<()>,
}

impl std::fmt::Debug for IggyConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IggyConsumer").finish()
    }
}

pub struct NextFuture<'a> {
    inner: flume::r#async::RecvFut<'a, StreamResult<IggyMessage, IggyErr>>,
}

impl std::fmt::Debug for NextFuture<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextFuture").finish()
    }
}

pub struct IggyMessageStream<'a> {
    inner: flume::r#async::RecvStream<'a, StreamResult<IggyMessage, IggyErr>>,
}

impl std::fmt::Debug for IggyMessageStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IggyMessageStream").finish()
    }
}

impl IggyConsumer {
    pub fn new(
        receiver: flume::Receiver<StreamResult<IggyMessage, IggyErr>>,
        handle: Arc<()>,
    ) -> Self {
        Self {
            receiver,
            _handle: handle,
        }
    }
}

impl Consumer for IggyConsumer {
    type Error = IggyErr;
    type Message<'a> = IggyMessage;
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = IggyMessageStream<'a>;

    async fn seek(&mut self, _to: Timestamp) -> StreamResult<(), IggyErr> {
        Err(StreamErr::Unsupported(
            "Iggy does not support seek by timestamp".into(),
        ))
    }

    async fn rewind(&mut self, _offset: SeqPos) -> StreamResult<(), IggyErr> {
        Err(StreamErr::Unsupported(
            "Iggy does not support rewind by offset in this adapter".into(),
        ))
    }

    fn assign(&mut self, _ss: (StreamKey, ShardId)) -> StreamResult<(), IggyErr> {
        Err(StreamErr::Unsupported(
            "Iggy uses stream/topic instead of shard assignment".into(),
        ))
    }

    fn unassign(&mut self, _ss: (StreamKey, ShardId)) -> StreamResult<(), IggyErr> {
        Err(StreamErr::Unsupported(
            "Iggy uses stream/topic instead of shard assignment".into(),
        ))
    }

    fn next(&self) -> Self::NextFuture<'_> {
        NextFuture {
            inner: self.receiver.recv_async(),
        }
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        IggyMessageStream {
            inner: self.receiver.stream(),
        }
    }
}

impl<'a> Future for NextFuture<'a> {
    type Output = StreamResult<IggyMessage, IggyErr>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        match Pin::new(&mut this.inner).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => {
                Poll::Ready(Err(StreamErr::Backend(IggyErr::ChannelRecv)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'a> Stream for IggyMessageStream<'a> {
    type Item = StreamResult<IggyMessage, IggyErr>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
