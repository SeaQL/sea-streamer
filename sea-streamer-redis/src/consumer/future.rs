use super::{AutoCommit, RedisConsumer};
use crate::{RedisErr, RedisResult};
use flume::r#async::RecvFut;
use sea_streamer_types::{export::futures::FutureExt, SharedMessage, StreamErr};
use std::{fmt::Debug, future::Future};

pub struct NextFuture<'a> {
    pub(super) con: &'a RedisConsumer,
    pub(super) fut: RecvFut<'a, RedisResult<SharedMessage>>,
}

impl<'a> Debug for NextFuture<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextFuture").finish()
    }
}

/// To avoid boxing the Future, this is a hand-unrolled version of the following:
///
/// ```ignore
/// let msg = self.receiver.recv_async().await?;
/// if self.auto_commit() {
///     self.handle.send_async()?;
/// }
/// Ok(msg)
/// ```
impl<'a> Future for NextFuture<'a> {
    type Output = RedisResult<SharedMessage>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};
        match self.fut.poll_unpin(cx) {
            Ready(res) => match res {
                Ok(Ok(msg)) => {
                    if self.con.options.auto_commit() == &AutoCommit::Delayed {
                        if self.con.ack(&msg).is_err() {
                            return Ready(Err(StreamErr::Backend(RedisErr::ConsumerDied)));
                        }
                    }
                    Ready(Ok(msg))
                }
                Ok(Err(err)) => Ready(Err(err)),
                Err(_) => Ready(Err(StreamErr::Backend(RedisErr::ConsumerDied))),
            },
            Pending => Pending,
        }
    }
}
