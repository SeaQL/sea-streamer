use super::RedisConsumer;
use crate::{RedisErr, RedisResult, consumer::cluster::CtrlMsg};
use flume::r#async::RecvFut;
use sea_streamer_types::{
    Consumer, SharedMessage, StreamErr,
    export::futures::{FutureExt, Stream},
};
use std::{fmt::Debug, future::Future};

pub struct NextFuture<'a> {
    pub(super) con: &'a RedisConsumer,
    pub(super) fut: RecvFut<'a, RedisResult<SharedMessage>>,
    pub(super) read: bool,
}

impl Debug for NextFuture<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextFuture").finish()
    }
}

impl Future for NextFuture<'_> {
    type Output = RedisResult<SharedMessage>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};
        if !self.read && !self.con.config.pre_fetch {
            self.read = true;
            self.con.handle.try_send(CtrlMsg::Read).ok();
        }
        match self.fut.poll_unpin(cx) {
            Ready(res) => match res {
                Ok(Ok(msg)) => {
                    if self.con.config.auto_ack && self.con.auto_ack(msg.header()).is_err() {
                        return Ready(Err(StreamErr::Backend(RedisErr::ConsumerDied)));
                    }
                    self.read = false;
                    Ready(Ok(msg))
                }
                Ok(Err(err)) => Ready(Err(err)),
                Err(_) => Ready(Err(StreamErr::Backend(RedisErr::ConsumerDied))),
            },
            Pending => Pending,
        }
    }
}

impl Drop for NextFuture<'_> {
    fn drop(&mut self) {
        if self.read {
            self.con.handle.try_send(CtrlMsg::Unread).ok();
        }
    }
}

pub struct StreamFuture<'a> {
    con: &'a RedisConsumer,
    fut: NextFuture<'a>,
}

impl Debug for StreamFuture<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamFuture").finish()
    }
}

impl<'a> StreamFuture<'a> {
    pub fn new(con: &'a RedisConsumer) -> Self {
        let fut = con.next();
        Self { con, fut }
    }
}

impl Stream for StreamFuture<'_> {
    type Item = RedisResult<SharedMessage>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll::{Pending, Ready};
        match std::pin::Pin::new(&mut self.fut).poll(cx) {
            Ready(res) => {
                self.fut = self.con.next();
                Ready(Some(res))
            }
            Pending => Pending,
        }
    }
}
