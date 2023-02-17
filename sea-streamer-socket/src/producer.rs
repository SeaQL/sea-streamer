use std::{future::Future, pin::Pin, task::Poll, time::Duration};

use sea_streamer_kafka::KafkaProducer;
use sea_streamer_stdio::StdioProducer;
use sea_streamer_types::{
    export::futures::FutureExt, Producer, Receipt, Sendable, StreamKey, StreamResult,
};

use crate::{map_err, Backend, BackendErr, SeaResult, SeaStreamerBackend};

#[derive(Debug, Clone)]
pub struct SeaProducer {
    pub(crate) backend: SeaProducerBackend,
}

#[derive(Debug, Clone)]
pub enum SeaProducerBackend {
    Kafka(KafkaProducer),
    Stdio(StdioProducer),
}

impl SeaStreamerBackend for SeaProducer {
    fn backend(&self) -> Backend {
        match self.backend {
            SeaProducerBackend::Kafka(_) => Backend::Kafka,
            SeaProducerBackend::Stdio(_) => Backend::Stdio,
        }
    }
}

#[derive(Debug)]
pub enum SendFuture {
    Kafka(sea_streamer_kafka::SendFuture),
    Stdio(sea_streamer_stdio::SendFuture),
}

impl Producer for SeaProducer {
    type Error = BackendErr;

    type SendFuture = SendFuture;

    fn send_to<S: Sendable>(&self, stream: &StreamKey, payload: S) -> SeaResult<Self::SendFuture> {
        Ok(match &self.backend {
            SeaProducerBackend::Kafka(i) => {
                SendFuture::Kafka(i.send_to(stream, payload).map_err(map_err)?)
            }
            SeaProducerBackend::Stdio(i) => {
                SendFuture::Stdio(i.send_to(stream, payload).map_err(map_err)?)
            }
        })
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

impl SeaProducer {
    // TODO may be we should add `flush` to the Producer trait
    pub async fn flush(self, timeout: Duration) -> SeaResult<()> {
        match self.backend {
            SeaProducerBackend::Kafka(i) => i.flush(timeout).await.map_err(map_err),
            SeaProducerBackend::Stdio(i) => i.flush().await.map_err(map_err),
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
