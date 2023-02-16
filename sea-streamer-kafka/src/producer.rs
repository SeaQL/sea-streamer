use std::{fmt::Debug, future::Future, time::Duration};

use crate::{
    cluster::cluster_uri, impl_into_string, stream_err, KafkaConnectOptions, KafkaErr, KafkaResult,
};
use rdkafka::{
    config::ClientConfig,
    producer::{
        DeliveryFuture, FutureProducer as RawProducer, FutureRecord as RawPayload,
        Producer as ProducerTrait,
    },
};
use sea_streamer_runtime::spawn_blocking;
use sea_streamer_types::{
    export::futures::FutureExt, runtime_error, MessageHeader, Producer, ProducerOptions, Sendable,
    ShardId, StreamErr, StreamKey, StreamResult, StreamerUri, Timestamp,
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
pub struct KafkaProducerOptions {
    compression_type: Option<CompressionType>,
}

pub struct SendFuture {
    stream_key: StreamKey,
    fut: DeliveryFuture,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KafkaProducerOptionKey {
    BootstrapServers,
    CompressionType,
}

type OptionKey = KafkaProducerOptionKey;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::None
    }
}

impl Debug for SendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFuture")
            .field("stream_key", &self.stream_key)
            .finish()
    }
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
    /// Flushes any pending messages. This method blocks
    pub(crate) fn flush_sync(&self, timeout: Duration) -> KafkaResult<()> {
        self.inner.flush(timeout).map_err(StreamErr::Backend)
    }

    pub async fn flush(self, timeout: Duration) -> KafkaResult<()> {
        spawn_blocking(move || self.flush_sync(timeout))
            .await
            .map_err(runtime_error)?
    }
}

impl ProducerOptions for KafkaProducerOptions {}

impl KafkaProducerOptions {
    /// Set the compression method for this producer
    pub fn set_compression_type(&mut self, v: CompressionType) -> &mut Self {
        self.compression_type = Some(v);
        self
    }
    pub fn compression_type(&self) -> Option<&CompressionType> {
        self.compression_type.as_ref()
    }

    fn make_client_config(&self, client_config: &mut ClientConfig) {
        if let Some(v) = self.compression_type {
            client_config.set(OptionKey::CompressionType, v);
        }
    }
}

impl OptionKey {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BootstrapServers => "bootstrap.servers",
            Self::CompressionType => "compression.type",
        }
    }
}

impl CompressionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

impl_into_string!(OptionKey);
impl_into_string!(CompressionType);

impl Future for SendFuture {
    type Output = StreamResult<MessageHeader, KafkaErr>;

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
                    Err((err, _)) => Err(stream_err(err)),
                },
                Err(_) => Err(stream_err(KafkaErr::Canceled)),
            }),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub(crate) fn create_producer(
    streamer: &StreamerUri,
    base_options: &KafkaConnectOptions,
    options: &KafkaProducerOptions,
) -> Result<KafkaProducer, KafkaErr> {
    let mut client_config = ClientConfig::new();
    client_config.set(OptionKey::BootstrapServers, cluster_uri(streamer)?);
    base_options.make_client_config(&mut client_config);
    options.make_client_config(&mut client_config);

    let producer = client_config.create()?;

    Ok(KafkaProducer {
        stream: None,
        inner: producer,
    })
}
