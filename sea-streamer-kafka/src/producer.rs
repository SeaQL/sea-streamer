use std::{fmt::Debug, future::Future};

use crate::{cluster::cluster_uri, KafkaErr, KafkaResult};
use rdkafka::{
    config::ClientConfig,
    producer::{
        DeliveryFuture, FutureProducer as RawProducer, FutureRecord as RawPayload,
        Producer as ProducerTrait,
    },
};
use sea_streamer::{
    export::futures::FutureExt, MessageHeader, Producer, ProducerOptions, Sendable, ShardId,
    StreamErr, StreamKey, StreamerUri, Timestamp,
};

#[derive(Clone)]
pub struct KafkaProducer {
    stream: Option<StreamKey>,
    inner: RawProducer,
}

impl Debug for KafkaProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProducer")
            .field("stream", &self.stream)
            .finish()
    }
}

#[derive(Debug, Default, Clone)]
pub struct KafkaProducerOptions {}

pub struct SendFuture {
    stream_key: StreamKey,
    fut: DeliveryFuture,
}

impl Producer for KafkaProducer {
    type Error = KafkaErr;
    type SendFuture = SendFuture;

    fn send_to<S: Sendable>(
        &self,
        stream: &StreamKey,
        payload: S,
    ) -> KafkaResult<Self::SendFuture> {
        let fut = self
            .inner
            .send_result(RawPayload::<str, [u8]>::to(stream.name()).payload(payload.as_bytes()))
            .map_err(|(err, _raw)| StreamErr::Backend(err))?;

        let stream_key = stream.to_owned();
        Ok(SendFuture { stream_key, fut })
    }

    fn anchor(&mut self, stream: StreamKey) -> KafkaResult<()> {
        if self.stream.is_none() {
            self.stream = Some(stream);
            Ok(())
        } else {
            Err(StreamErr::AlreadyAnchored)
        }
    }

    fn anchored(&self) -> KafkaResult<&StreamKey> {
        if let Some(stream) = &self.stream {
            Ok(stream)
        } else {
            Err(StreamErr::NotAnchored)
        }
    }
}

impl KafkaProducer {
    pub fn flush(&self, timeout: std::time::Duration) -> KafkaResult<()> {
        self.inner.flush(timeout).map_err(StreamErr::Backend)
    }
}

impl ProducerOptions for KafkaProducerOptions {}

impl Future for SendFuture {
    type Output = Result<MessageHeader, KafkaErr>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let stream_key = self.stream_key.to_owned();
        match self.fut.poll_unpin(cx) {
            std::task::Poll::Ready(res) => std::task::Poll::Ready(match res {
                Ok(res) => match res {
                    Ok((part, offset)) => Ok(MessageHeader::new(
                        stream_key,
                        ShardId::new(part as u64),
                        offset as u64,
                        Timestamp::now_utc(),
                    )),
                    Err((err, _)) => Err(err),
                },
                Err(_) => Err(KafkaErr::Canceled),
            }),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub(crate) fn create_producer(streamer: &StreamerUri) -> Result<KafkaProducer, KafkaErr> {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", cluster_uri(streamer));
    client_config.set("message.max.bytes", "1000000000"); // ~1Gb - this is the max that rdkafka allows

    let inner = client_config.create()?;

    Ok(KafkaProducer {
        stream: None,
        inner,
    })
}
