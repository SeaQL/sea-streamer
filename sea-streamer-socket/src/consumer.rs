use std::{fmt::Debug, future::Future, pin::Pin, task::Poll};

use sea_streamer_kafka::KafkaConsumer;
use sea_streamer_stdio::StdioConsumer;
use sea_streamer_types::{
    export::{
        async_trait,
        futures::{FutureExt, Stream},
    },
    Consumer, SeqPos, ShardId, StreamResult, Timestamp,
};

use crate::{map_err, Backend, BackendErr, SeaMessage, SeaResult, SeaStreamerBackend};

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of Consumer.
pub struct SeaConsumer {
    pub(crate) backend: SeaConsumerBackend,
}

#[derive(Debug)]
pub(crate) enum SeaConsumerBackend {
    Kafka(KafkaConsumer),
    Stdio(StdioConsumer),
}

/// `sea-streamer-socket` concrete type of Future that will yield the next message.
pub enum NextFuture<'a> {
    Kafka(sea_streamer_kafka::NextFuture<'a>),
    Stdio(sea_streamer_stdio::NextFuture<'a>),
}

/// `sea-streamer-socket` concrete type of Stream that will yield the next messages.
pub enum SeaMessageStream<'a> {
    Kafka(sea_streamer_kafka::KafkaMessageStream<'a>),
    Stdio(sea_streamer_stdio::StdioMessageStream<'a>),
}

impl<'a> Debug for NextFuture<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kafka(_) => write!(f, "NextFuture::Kafka"),
            Self::Stdio(_) => write!(f, "NextFuture::Stdio"),
        }
    }
}

impl<'a> Debug for SeaMessageStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kafka(_) => write!(f, "KafkaMessageStream"),
            Self::Stdio(_) => write!(f, "StdioMessageStream"),
        }
    }
}

impl SeaStreamerBackend for SeaConsumer {
    type Kafka = KafkaConsumer;
    type Stdio = StdioConsumer;

    fn backend(&self) -> Backend {
        match self.backend {
            SeaConsumerBackend::Kafka(_) => Backend::Kafka,
            SeaConsumerBackend::Stdio(_) => Backend::Stdio,
        }
    }

    fn get_kafka(&mut self) -> Option<&mut Self::Kafka> {
        match &mut self.backend {
            SeaConsumerBackend::Kafka(s) => Some(s),
            SeaConsumerBackend::Stdio(_) => None,
        }
    }

    fn get_stdio(&mut self) -> Option<&mut Self::Stdio> {
        match &mut self.backend {
            SeaConsumerBackend::Kafka(_) => None,
            SeaConsumerBackend::Stdio(s) => Some(s),
        }
    }
}

#[async_trait]
impl Consumer for SeaConsumer {
    type Error = BackendErr;
    type Message<'a> = SeaMessage<'a>;
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = SeaMessageStream<'a>;

    async fn seek(&mut self, to: Timestamp) -> SeaResult<()> {
        match &mut self.backend {
            SeaConsumerBackend::Kafka(i) => i.seek(to).await.map_err(map_err),
            SeaConsumerBackend::Stdio(i) => i.seek(to).await.map_err(map_err),
        }
    }

    fn rewind(&mut self, offset: SeqPos) -> SeaResult<()> {
        match &mut self.backend {
            SeaConsumerBackend::Kafka(i) => i.rewind(offset).map_err(map_err),
            SeaConsumerBackend::Stdio(i) => i.rewind(offset).map_err(map_err),
        }
    }

    fn assign(&mut self, shard: ShardId) -> SeaResult<()> {
        match &mut self.backend {
            SeaConsumerBackend::Kafka(i) => i.assign(shard).map_err(map_err),
            SeaConsumerBackend::Stdio(i) => i.assign(shard).map_err(map_err),
        }
    }

    fn next(&self) -> Self::NextFuture<'_> {
        match &self.backend {
            SeaConsumerBackend::Kafka(i) => NextFuture::Kafka(i.next()),
            SeaConsumerBackend::Stdio(i) => NextFuture::Stdio(i.next()),
        }
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        match &mut self.backend {
            SeaConsumerBackend::Kafka(i) => SeaMessageStream::Kafka(i.stream()),
            SeaConsumerBackend::Stdio(i) => SeaMessageStream::Stdio(i.stream()),
        }
    }
}

impl<'a> Future for NextFuture<'a> {
    type Output = StreamResult<SeaMessage<'a>, BackendErr>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match Pin::into_inner(self) {
            Self::Kafka(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map(SeaMessage::Kafka).map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
            Self::Stdio(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map(SeaMessage::Stdio).map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl<'a> Stream for SeaMessageStream<'a> {
    type Item = StreamResult<SeaMessage<'a>, BackendErr>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match Pin::into_inner(self) {
            Self::Kafka(ss) => match Pin::new(ss).poll_next(cx) {
                Poll::Ready(Some(res)) => {
                    Poll::Ready(Some(res.map(SeaMessage::Kafka).map_err(map_err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            Self::Stdio(ss) => match Pin::new(ss).poll_next(cx) {
                Poll::Ready(Some(res)) => {
                    Poll::Ready(Some(res.map(SeaMessage::Stdio).map_err(map_err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
