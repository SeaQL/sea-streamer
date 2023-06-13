//! Basically copied from sea-streamer-redis
use super::{FileConsumer, NextFuture};
use crate::FileResult;
use sea_streamer_types::{export::futures::Stream, Consumer, SharedMessage};
use std::{fmt::Debug, future::Future};

pub struct StreamFuture<'a> {
    con: &'a FileConsumer,
    fut: NextFuture<'a>,
}

impl<'a> Debug for StreamFuture<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamFuture").finish()
    }
}

impl<'a> StreamFuture<'a> {
    pub fn new(con: &'a FileConsumer) -> Self {
        let fut = con.next();
        Self { con, fut }
    }
}

impl<'a> Stream for StreamFuture<'a> {
    type Item = FileResult<SharedMessage>;

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
