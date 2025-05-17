#[cfg(feature = "backend-file")]
use sea_streamer_file::FileConsumer;
#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::KafkaConsumer;
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::RedisConsumer;
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioConsumer;

use crate::{Backend, BackendErr, SeaMessage, SeaResult, SeaStreamerBackend, map_err};
use sea_streamer_types::{
    Consumer, SeqPos, ShardId, StreamKey, StreamResult, Timestamp,
    export::futures::{FutureExt, Stream},
};
use std::{fmt::Debug, future::Future, pin::Pin, task::Poll};

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of Consumer.
pub struct SeaConsumer {
    pub(crate) backend: SeaConsumerBackend,
}

#[derive(Debug)]
pub(crate) enum SeaConsumerBackend {
    #[cfg(feature = "backend-kafka")]
    Kafka(KafkaConsumer),
    #[cfg(feature = "backend-redis")]
    Redis(RedisConsumer),
    #[cfg(feature = "backend-stdio")]
    Stdio(StdioConsumer),
    #[cfg(feature = "backend-file")]
    File(FileConsumer),
}

/// `sea-streamer-socket` concrete type of Future that will yield the next message.
pub enum NextFuture<'a> {
    #[cfg(feature = "backend-kafka")]
    Kafka(sea_streamer_kafka::NextFuture<'a>),
    #[cfg(feature = "backend-redis")]
    Redis(sea_streamer_redis::NextFuture<'a>),
    #[cfg(feature = "backend-stdio")]
    Stdio(sea_streamer_stdio::NextFuture<'a>),
    #[cfg(feature = "backend-file")]
    File(sea_streamer_file::NextFuture<'a>),
}

/// `sea-streamer-socket` concrete type of Stream that will yield the next messages.
pub enum SeaMessageStream<'a> {
    #[cfg(feature = "backend-kafka")]
    Kafka(sea_streamer_kafka::KafkaMessageStream<'a>),
    #[cfg(feature = "backend-redis")]
    Redis(sea_streamer_redis::RedisMessageStream<'a>),
    #[cfg(feature = "backend-stdio")]
    Stdio(sea_streamer_stdio::StdioMessageStream<'a>),
    #[cfg(feature = "backend-file")]
    File(sea_streamer_file::FileMessageStream<'a>),
}

impl Debug for NextFuture<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => write!(f, "NextFuture::Kafka"),
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => write!(f, "NextFuture::Redis"),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => write!(f, "NextFuture::Stdio"),
            #[cfg(feature = "backend-file")]
            Self::File(_) => write!(f, "NextFuture::File"),
        }
    }
}

impl Debug for SeaMessageStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(_) => write!(f, "KafkaMessageStream"),
            #[cfg(feature = "backend-redis")]
            Self::Redis(_) => write!(f, "RedisMessageStream"),
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(_) => write!(f, "StdioMessageStream"),
            #[cfg(feature = "backend-file")]
            Self::File(_) => write!(f, "FileMessageStream"),
        }
    }
}

#[cfg(feature = "backend-kafka")]
impl From<KafkaConsumer> for SeaConsumer {
    fn from(i: KafkaConsumer) -> Self {
        Self {
            backend: SeaConsumerBackend::Kafka(i),
        }
    }
}

#[cfg(feature = "backend-redis")]
impl From<RedisConsumer> for SeaConsumer {
    fn from(i: RedisConsumer) -> Self {
        Self {
            backend: SeaConsumerBackend::Redis(i),
        }
    }
}

#[cfg(feature = "backend-stdio")]
impl From<StdioConsumer> for SeaConsumer {
    fn from(i: StdioConsumer) -> Self {
        Self {
            backend: SeaConsumerBackend::Stdio(i),
        }
    }
}

#[cfg(feature = "backend-file")]
impl From<FileConsumer> for SeaConsumer {
    fn from(i: FileConsumer) -> Self {
        Self {
            backend: SeaConsumerBackend::File(i),
        }
    }
}

impl SeaStreamerBackend for SeaConsumer {
    #[cfg(feature = "backend-kafka")]
    type Kafka = KafkaConsumer;
    #[cfg(feature = "backend-redis")]
    type Redis = RedisConsumer;
    #[cfg(feature = "backend-stdio")]
    type Stdio = StdioConsumer;
    #[cfg(feature = "backend-file")]
    type File = FileConsumer;

    fn backend(&self) -> Backend {
        match self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(_) => Backend::Kafka,
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(_) => Backend::Redis,
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(_) => Backend::Stdio,
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(_) => Backend::File,
        }
    }

    #[cfg(feature = "backend-kafka")]
    fn get_kafka(&mut self) -> Option<&mut Self::Kafka> {
        match &mut self.backend {
            SeaConsumerBackend::Kafka(s) => Some(s),
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(_) => None,
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(_) => None,
        }
    }

    #[cfg(feature = "backend-redis")]
    fn get_redis(&mut self) -> Option<&mut Self::Redis> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(_) => None,
            SeaConsumerBackend::Redis(s) => Some(s),
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(_) => None,
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(_) => None,
        }
    }

    #[cfg(feature = "backend-stdio")]
    fn get_stdio(&mut self) -> Option<&mut Self::Stdio> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(_) => None,
            SeaConsumerBackend::Stdio(s) => Some(s),
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(_) => None,
        }
    }

    #[cfg(feature = "backend-file")]
    fn get_file(&mut self) -> Option<&mut Self::File> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(_) => None,
            SeaConsumerBackend::File(s) => Some(s),
        }
    }
}

impl Consumer for SeaConsumer {
    type Error = BackendErr;
    type Message<'a> = SeaMessage<'a>;
    type NextFuture<'a> = NextFuture<'a>;
    type Stream<'a> = SeaMessageStream<'a>;

    async fn seek(&mut self, to: Timestamp) -> SeaResult<()> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(i) => i.seek(to).await.map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(i) => i.seek(to).await.map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(i) => i.seek(to).await.map_err(map_err),
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(i) => i.seek(to).await.map_err(map_err),
        }
    }

    async fn rewind(&mut self, pos: SeqPos) -> SeaResult<()> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(i) => i.rewind(pos).await.map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(i) => i.rewind(pos).await.map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(i) => i.rewind(pos).await.map_err(map_err),
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(i) => i.rewind(pos).await.map_err(map_err),
        }
    }

    fn assign(&mut self, ss: (StreamKey, ShardId)) -> SeaResult<()> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(i) => i.assign(ss).map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(i) => i.assign(ss).map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(i) => i.assign(ss).map_err(map_err),
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(i) => i.assign(ss).map_err(map_err),
        }
    }

    fn unassign(&mut self, ss: (StreamKey, ShardId)) -> SeaResult<()> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(i) => i.unassign(ss).map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(i) => i.unassign(ss).map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(i) => i.unassign(ss).map_err(map_err),
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(i) => i.unassign(ss).map_err(map_err),
        }
    }

    fn next(&self) -> Self::NextFuture<'_> {
        match &self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(i) => NextFuture::Kafka(i.next()),
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(i) => NextFuture::Redis(i.next()),
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(i) => NextFuture::Stdio(i.next()),
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(i) => NextFuture::File(i.next()),
        }
    }

    fn stream<'a, 'b: 'a>(&'b mut self) -> Self::Stream<'a> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaConsumerBackend::Kafka(i) => SeaMessageStream::Kafka(i.stream()),
            #[cfg(feature = "backend-redis")]
            SeaConsumerBackend::Redis(i) => SeaMessageStream::Redis(i.stream()),
            #[cfg(feature = "backend-stdio")]
            SeaConsumerBackend::Stdio(i) => SeaMessageStream::Stdio(i.stream()),
            #[cfg(feature = "backend-file")]
            SeaConsumerBackend::File(i) => SeaMessageStream::File(i.stream()),
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
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map(SeaMessage::Kafka).map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-redis")]
            Self::Redis(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map(SeaMessage::Redis).map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map(SeaMessage::Stdio).map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-file")]
            Self::File(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map(SeaMessage::File).map_err(map_err)),
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
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(ss) => match Pin::new(ss).poll_next(cx) {
                Poll::Ready(Some(res)) => {
                    Poll::Ready(Some(res.map(SeaMessage::Kafka).map_err(map_err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-redis")]
            Self::Redis(ss) => match Pin::new(ss).poll_next(cx) {
                Poll::Ready(Some(res)) => {
                    Poll::Ready(Some(res.map(SeaMessage::Redis).map_err(map_err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(ss) => match Pin::new(ss).poll_next(cx) {
                Poll::Ready(Some(res)) => {
                    Poll::Ready(Some(res.map(SeaMessage::Stdio).map_err(map_err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-file")]
            Self::File(ss) => match Pin::new(ss).poll_next(cx) {
                Poll::Ready(Some(res)) => {
                    Poll::Ready(Some(res.map(SeaMessage::File).map_err(map_err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
