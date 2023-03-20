use std::{future::Future, pin::Pin, task::Poll};

use sea_streamer_kafka::KafkaProducer;
use sea_streamer_stdio::StdioProducer;
use sea_streamer_types::{
    export::{async_trait, futures::FutureExt},
    Buffer, Producer, Receipt, StreamKey, StreamResult,
};

use crate::{map_err, Backend, BackendErr, SeaResult, SeaStreamerBackend};

#[derive(Debug, Clone)]
/// `sea-streamer-socket` concrete type of Producer.
pub struct SeaProducer {
    pub(crate) backend: SeaProducerBackend,
}

#[derive(Debug, Clone)]
pub(crate) enum SeaProducerBackend {
    Kafka(KafkaProducer),
    Stdio(StdioProducer),
}

impl From<KafkaProducer> for SeaProducer {
    fn from(i: KafkaProducer) -> Self {
        Self {
            backend: SeaProducerBackend::Kafka(i),
        }
    }
}

impl From<StdioProducer> for SeaProducer {
    fn from(i: StdioProducer) -> Self {
        Self {
            backend: SeaProducerBackend::Stdio(i),
        }
    }
}

impl SeaStreamerBackend for SeaProducer {
    type Kafka = KafkaProducer;
    type Stdio = StdioProducer;

    fn backend(&self) -> Backend {
        match self.backend {
            SeaProducerBackend::Kafka(_) => Backend::Kafka,
            SeaProducerBackend::Stdio(_) => Backend::Stdio,
        }
    }

    fn get_kafka(&mut self) -> Option<&mut Self::Kafka> {
        match &mut self.backend {
            SeaProducerBackend::Kafka(s) => Some(s),
            SeaProducerBackend::Stdio(_) => None,
        }
    }

    fn get_stdio(&mut self) -> Option<&mut Self::Stdio> {
        match &mut self.backend {
            SeaProducerBackend::Kafka(_) => None,
            SeaProducerBackend::Stdio(s) => Some(s),
        }
    }
}

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of a Future that will yield a send receipt.
pub enum SendFuture {
    Kafka(sea_streamer_kafka::SendFuture),
    Stdio(sea_streamer_stdio::SendFuture),
}

#[async_trait]
impl Producer for SeaProducer {
    type Error = BackendErr;

    type SendFuture = SendFuture;

    fn send_to<S: Buffer>(&self, stream: &StreamKey, payload: S) -> SeaResult<Self::SendFuture> {
        Ok(match &self.backend {
            SeaProducerBackend::Kafka(i) => {
                SendFuture::Kafka(i.send_to(stream, payload).map_err(map_err)?)
            }
            SeaProducerBackend::Stdio(i) => {
                SendFuture::Stdio(i.send_to(stream, payload).map_err(map_err)?)
            }
        })
    }

    async fn end(self) -> SeaResult<()> {
        match self.backend {
            SeaProducerBackend::Kafka(i) => i.end().await.map_err(map_err),
            SeaProducerBackend::Stdio(i) => i.end().await.map_err(map_err),
        }
    }

    async fn flush(&mut self) -> SeaResult<()> {
        match &mut self.backend {
            SeaProducerBackend::Kafka(i) => i.flush().await.map_err(map_err),
            SeaProducerBackend::Stdio(i) => i.flush().await.map_err(map_err),
        }
    }

    fn anchor(&mut self, stream: StreamKey) -> SeaResult<()> {
        match &mut self.backend {
            SeaProducerBackend::Kafka(i) => i.anchor(stream).map_err(map_err),
            SeaProducerBackend::Stdio(i) => i.anchor(stream).map_err(map_err),
        }
    }

    fn anchored(&self) -> SeaResult<&StreamKey> {
        match &self.backend {
            SeaProducerBackend::Kafka(i) => i.anchored().map_err(map_err),
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
            Self::Kafka(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
            Self::Stdio(fut) => match Pin::new(fut).poll_unpin(cx) {
                Poll::Ready(res) => Poll::Ready(res.map_err(map_err)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
