use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use iggy::prelude::MessageClient;
use sea_streamer_types::{
    Buffer, MessageHeader, Producer, Receipt, SeqNo, ShardId, StreamErr, StreamKey, StreamResult,
    Timestamp,
};
use tokio::sync::Mutex;

use crate::error::IggyErr;

#[derive(Clone)]
pub struct IggyProducer {
    inner: Arc<Mutex<IggyProducerInner>>,
    anchor: Arc<std::sync::Mutex<Option<StreamKey>>>,
}

struct IggyProducerInner {
    client: Arc<iggy::prelude::IggyClient>,
    stream_name: String,
    topic_name: String,
}

impl std::fmt::Debug for IggyProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IggyProducer").finish()
    }
}

pub struct SendFuture {
    inner: Pin<Box<dyn Future<Output = StreamResult<Receipt, IggyErr>> + Send>>,
}

impl std::fmt::Debug for SendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFuture").finish()
    }
}

impl IggyProducer {
    pub(crate) fn new(
        client: Arc<iggy::prelude::IggyClient>,
        stream_name: String,
        topic_name: String,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(IggyProducerInner {
                client,
                stream_name,
                topic_name,
            })),
            anchor: Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

impl Producer for IggyProducer {
    type Error = IggyErr;
    type SendFuture = SendFuture;

    fn send_to<S: Buffer>(
        &self,
        stream: &StreamKey,
        payload: S,
    ) -> StreamResult<Self::SendFuture, IggyErr> {
        let bytes = payload.into_bytes();
        let inner = self.inner.clone();
        let stream_key = stream.clone();

        Ok(SendFuture {
            inner: Box::pin(async move {
                let inner = inner.lock().await;
                let topic_id: iggy::prelude::Identifier = inner
                    .topic_name
                    .as_str()
                    .try_into()
                    .map_err(|e| StreamErr::Backend(IggyErr::Client(e)))?;
                let stream_id: iggy::prelude::Identifier = inner
                    .stream_name
                    .as_str()
                    .try_into()
                    .map_err(|e| StreamErr::Backend(IggyErr::Client(e)))?;

                let message = iggy::prelude::IggyMessage::builder()
                    .payload(bytes.into())
                    .build()
                    .map_err(|e| StreamErr::Backend(IggyErr::Client(e)))?;

                MessageClient::send_messages(
                    &*inner.client,
                    &stream_id,
                    &topic_id,
                    &iggy::prelude::Partitioning::balanced(),
                    &mut [message],
                )
                .await
                .map_err(|e| StreamErr::Backend(IggyErr::Client(e)))?;

                let header = MessageHeader::new(
                    stream_key,
                    ShardId::new(0),
                    SeqNo::default(),
                    Timestamp::now_utc(),
                );
                Ok(header)
            }),
        })
    }

    async fn end(self) -> StreamResult<(), IggyErr> {
        Ok(())
    }

    async fn flush(&mut self) -> StreamResult<(), IggyErr> {
        Ok(())
    }

    fn anchor(&mut self, stream: StreamKey) -> StreamResult<(), IggyErr> {
        let mut anchor = self.anchor.lock().unwrap();
        if anchor.is_some() {
            return Err(StreamErr::AlreadyAnchored);
        }
        *anchor = Some(stream);
        Ok(())
    }

    fn anchored(&self) -> StreamResult<&StreamKey, IggyErr> {
        Err(StreamErr::NotAnchored)
    }
}

impl Future for SendFuture {
    type Output = StreamResult<Receipt, IggyErr>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
