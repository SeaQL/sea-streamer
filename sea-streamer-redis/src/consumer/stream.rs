use super::{ConsumerConfig, CtrlMsg, RedisConsumer};
use crate::{RedisErr, RedisResult};
use flume::{r#async::RecvStream, Sender};
use sea_streamer_types::{export::futures::Stream, SharedMessage, StreamErr};
use std::{fmt::Debug, pin::Pin, task::Poll};

pub struct RedisMessStream<'a> {
    pub(super) config: ConsumerConfig,
    pub(super) stream: RecvStream<'a, RedisResult<SharedMessage>>,
    pub(super) handle: Sender<CtrlMsg>,
    pub(super) read: bool,
}

impl<'a> Debug for RedisMessStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisMessStream").finish()
    }
}

// logic must mirror that of sea-streamer-redis/src/consumer/future.rs

impl<'a> Stream for RedisMessStream<'a> {
    type Item = RedisResult<SharedMessage>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        use std::task::Poll::{Pending, Ready};
        if !self.read && !self.config.pre_fetch {
            self.read = true;
            self.handle.try_send(CtrlMsg::Read).ok();
        }
        match Pin::new(&mut self.stream).poll_next(cx) {
            Ready(res) => match res {
                Some(Ok(msg)) => {
                    if self.config.auto_ack
                        && RedisConsumer::auto_ack_static(&self.handle, msg.header()).is_err()
                    {
                        return Ready(Some(Err(StreamErr::Backend(RedisErr::ConsumerDied))));
                    }
                    self.read = false;
                    Ready(Some(Ok(msg)))
                }
                Some(Err(err)) => Ready(Some(Err(err))),
                None => Ready(Some(Err(StreamErr::Backend(RedisErr::ConsumerDied)))),
            },
            Pending => Pending,
        }
    }
}

impl<'a> Drop for RedisMessStream<'a> {
    fn drop(&mut self) {
        if self.read {
            self.handle.try_send(CtrlMsg::Unread).ok();
        }
    }
}
