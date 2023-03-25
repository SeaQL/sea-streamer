#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::KafkaProducer;
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::RedisProducer;
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioProducer;

use crate::{map_err, Backend, BackendErr, SeaResult, SeaStreamerBackend};
use sea_streamer_types::{
    export::{async_trait, futures::FutureExt},
    Buffer, Producer, Receipt, StreamKey, StreamResult,
};
use std::{future::Future, pin::Pin, task::Poll};

#[derive(Debug, Clone)]
/// `sea-streamer-socket` concrete type of Producer.
pub struct SeaProducer {
    pub(crate) backend: SeaProducerBackend,
}

#[derive(Debug, Clone)]
pub(crate) enum SeaProducerBackend {
    #[cfg(feature = "backend-kafka")]
    Kafka(KafkaProducer),
    #[cfg(feature = "backend-redis")]
    Redis(RedisProducer),
    #[cfg(feature = "backend-stdio")]
    Stdio(StdioProducer),
}

#[cfg(feature = "backend-kafka")]
impl From<KafkaProducer> for SeaProducer {
    fn from(i: KafkaProducer) -> Self {
        Self {
            backend: SeaProducerBackend::Kafka(i),
        }
    }
}

#[cfg(feature = "backend-redis")]
impl From<RedisProducer> for SeaProducer {
    fn from(i: RedisProducer) -> Self {
        Self {
            backend: SeaProducerBackend::Redis(i),
        }
    }
}

#[cfg(feature = "backend-stdio")]
impl From<StdioProducer> for SeaProducer {
    fn from(i: StdioProducer) -> Self {
        Self {
            backend: SeaProducerBackend::Stdio(i),
        }
    }
}

impl SeaStreamerBackend for SeaProducer {
    #[cfg(feature = "backend-kafka")]
    type Kafka = KafkaProducer;
    #[cfg(feature = "backend-redis")]
    type Redis = RedisProducer;
    #[cfg(feature = "backend-stdio")]
    type Stdio = StdioProducer;

    fn backend(&self) -> Backend {
        match self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(_) => Backend::Kafka,
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(_) => Backend::Redis,
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(_) => Backend::Stdio,
        }
    }

    #[cfg(feature = "backend-kafka")]
    fn get_kafka(&mut self) -> Option<&mut KafkaProducer> {
        match &mut self.backend {
            SeaProducerBackend::Kafka(s) => Some(s),
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(_) => None,
        }
    }

    #[cfg(feature = "backend-redis")]
    fn get_redis(&mut self) -> Option<&mut RedisProducer> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(_) => None,
            SeaProducerBackend::Redis(s) => Some(s),
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(_) => None,
        }
    }

    #[cfg(feature = "backend-stdio")]
    fn get_stdio(&mut self) -> Option<&mut StdioProducer> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(_) => None,
            SeaProducerBackend::Stdio(s) => Some(s),
        }
    }
}

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of a Future that will yield a send receipt.
pub enum SendFuture {
    #[cfg(feature = "backend-kafka")]
    Kafka(sea_streamer_kafka::SendFuture),
    #[cfg(feature = "backend-redis")]
    Redis(sea_streamer_redis::SendFuture),
    #[cfg(feature = "backend-stdio")]
    Stdio(sea_streamer_stdio::SendFuture),
}

#[async_trait]
impl Producer for SeaProducer {
    type Error = BackendErr;

    type SendFuture = SendFuture;

    fn send_to<S: Buffer>(&self, stream: &StreamKey, payload: S) -> SeaResult<Self::SendFuture> {
        Ok(match &self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(i) => {
                SendFuture::Kafka(i.send_to(stream, payload).map_err(map_err)?)
            }
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(i) => {
                SendFuture::Redis(i.send_to(stream, payload).map_err(map_err)?)
            }
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(i) => {
                SendFuture::Stdio(i.send_to(stream, payload).map_err(map_err)?)
            }
        })
    }

    async fn end(self) -> SeaResult<()> {
        match self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(i) => i.end().await.map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(i) => i.end().await.map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(i) => i.end().await.map_err(map_err),
        }
    }

    async fn flush(&mut self) -> SeaResult<()> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(i) => i.flush().await.map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(i) => i.flush().await.map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(i) => i.flush().await.map_err(map_err),
        }
    }

    fn anchor(&mut self, stream: StreamKey) -> SeaResult<()> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(i) => i.anchor(stream).map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(i) => i.anchor(stream).map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(i) => i.anchor(stream).map_err(map_err),
        }
    }

    fn anchored(&self) -> SeaResult<&StreamKey> {
        match &self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaProducerBackend::Kafka(i) => i.anchored().map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaProducerBackend::Redis(i) => i.anchored().map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaProducerBackend::Stdio(i) => i.anchored().map_err(map_err),
        }
    }
}

impl Future for SendFuture {
    type Output = StreamResult<Receipt, BackendErr>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match Pin::into_inner(self) {
            #[cfg(feature = "backend-kafka")]
            Self::Kafka(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-redis")]
            Self::Redis(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
            #[cfg(feature = "backend-stdio")]
            Self::Stdio(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
