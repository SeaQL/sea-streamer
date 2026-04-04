use std::sync::Arc;

use iggy::prelude::{Client, MessageClient, StreamClient, TopicClient};
use sea_streamer_types::{StreamErr, StreamKey, StreamResult, Streamer, StreamerUri};

use crate::consumer::IggyConsumer;
use crate::error::IggyErr;
use crate::message::IggyMessage;
use crate::options::{IggyConnectOptions, IggyConsumerOptions, IggyProducerOptions};
use crate::producer::IggyProducer;

#[derive(Debug, Clone)]
pub struct IggyStreamer {
    client: Arc<iggy::prelude::IggyClient>,
}

impl Streamer for IggyStreamer {
    type Error = IggyErr;
    type Producer = IggyProducer;
    type Consumer = IggyConsumer;
    type ConnectOptions = IggyConnectOptions;
    type ConsumerOptions = IggyConsumerOptions;
    type ProducerOptions = IggyProducerOptions;

    async fn connect(
        uri: StreamerUri,
        options: Self::ConnectOptions,
    ) -> StreamResult<Self, IggyErr> {
        let url = if let Some(node) = uri.nodes().first() {
            node.to_string()
        } else {
            options.url().to_owned()
        };

        let client = iggy::prelude::IggyClient::from_connection_string(&url)
            .map_err(|e| StreamErr::Connect(format!("Iggy connect failed: {e}")))?;

        Client::connect(&client)
            .await
            .map_err(|e| StreamErr::Connect(format!("Iggy connect failed: {e}")))?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    async fn disconnect(self) -> StreamResult<(), IggyErr> {
        Client::shutdown(&*self.client)
            .await
            .map_err(|e| StreamErr::Backend(IggyErr::Client(e)))?;
        Ok(())
    }

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer, IggyErr> {
        let stream_name = options
            .stream_name()
            .unwrap_or("default")
            .to_owned();
        let topic_name = options
            .topic_name()
            .unwrap_or("default")
            .to_owned();

        Ok(IggyProducer::new(
            self.client.clone(),
            stream_name,
            topic_name,
        ))
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> StreamResult<Self::Consumer, IggyErr> {
        let stream_name = if let Some(name) = options.stream_name() {
            name.to_owned()
        } else if let Some(first) = streams.first() {
            first.name().to_owned()
        } else {
            return Err(StreamErr::Backend(IggyErr::StreamTopicRequired));
        };

        let topic_name = if let Some(name) = options.topic_name() {
            name.to_owned()
        } else if streams.len() >= 2 {
            streams[1].name().to_owned()
        } else {
            return Err(StreamErr::Backend(IggyErr::StreamTopicRequired));
        };

        let (sender, receiver) = flume::bounded(1024);
        let client = self.client.clone();
        let batch_size = options.batch_size();
        let polling_interval_ms = options.polling_interval_ms();
        let handle = Arc::new(());
        let weak = Arc::downgrade(&handle);

        tokio::spawn(async move {
            let Ok(stream_id): Result<iggy::prelude::Identifier, _> =
                stream_name.as_str().try_into()
            else {
                let _ = sender.send(Err(StreamErr::Backend(IggyErr::Generic(format!(
                    "invalid stream name: {stream_name}"
                )))));
                return;
            };
            let Ok(topic_id): Result<iggy::prelude::Identifier, _> =
                topic_name.as_str().try_into()
            else {
                let _ = sender.send(Err(StreamErr::Backend(IggyErr::Generic(format!(
                    "invalid topic name: {topic_name}"
                )))));
                return;
            };

            let consumer = iggy::prelude::Consumer::default();
            let polling_strategy = iggy::prelude::PollingStrategy::next();
            let interval = std::time::Duration::from_millis(polling_interval_ms);

            loop {
                if weak.upgrade().is_none() {
                    break;
                }

                let poll_result = MessageClient::poll_messages(
                    &*client,
                    &stream_id,
                    &topic_id,
                    None,
                    &consumer,
                    &polling_strategy,
                    batch_size,
                    false,
                )
                .await;

                match poll_result {
                    Ok(polled) => {
                        for msg in polled.messages {
                            let payload = msg.payload.to_vec();
                            let Ok(stream_key) = StreamKey::new(&topic_name) else {
                                continue;
                            };
                            let iggy_msg = IggyMessage::new(
                                stream_key,
                                sea_streamer_types::ShardId::new(0),
                                msg.header.offset,
                                sea_streamer_types::Timestamp::now_utc(),
                                payload,
                            );
                            if sender.send(Ok(iggy_msg)).is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Iggy poll error: {e}");
                    }
                }

                tokio::time::sleep(interval).await;
            }
        });

        Ok(IggyConsumer::new(receiver, handle))
    }
}
