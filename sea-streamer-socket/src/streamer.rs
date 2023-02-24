use sea_streamer_kafka::KafkaStreamer;
use sea_streamer_stdio::StdioStreamer;
use sea_streamer_types::{export::async_trait, StreamErr, StreamKey, Streamer, StreamerUri};

use crate::{
    map_err, Backend, BackendErr, SeaConnectOptions, SeaConnectOptionsTrait, SeaConsumer,
    SeaConsumerBackend, SeaConsumerOptions, SeaProducer, SeaProducerBackend, SeaProducerOptions,
    SeaProducerOptionsTrait, SeaResult, SeaStreamerBackend,
};

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of Streamer.
pub struct SeaStreamer {
    backend: SeaStreamerInner,
}

#[derive(Debug)]
enum SeaStreamerInner {
    Kafka(KafkaStreamer),
    Stdio(StdioStreamer),
}

impl SeaStreamerBackend for SeaStreamer {
    fn backend(&self) -> Backend {
        match self.backend {
            SeaStreamerInner::Kafka(_) => Backend::Kafka,
            SeaStreamerInner::Stdio(_) => Backend::Stdio,
        }
    }
}

#[async_trait]
impl Streamer for SeaStreamer {
    type Error = BackendErr;
    type Producer = SeaProducer;
    type Consumer = SeaConsumer;
    type ConnectOptions = SeaConnectOptions;
    type ConsumerOptions = SeaConsumerOptions;
    type ProducerOptions = SeaProducerOptions;

    /// Nothing will happen until you create a producer/consumer
    async fn connect(uri: StreamerUri, options: Self::ConnectOptions) -> SeaResult<Self> {
        let backend = match uri.protocol() {
            Some(protocol) => match protocol {
                "kafka" => SeaStreamerInner::Kafka(
                    KafkaStreamer::connect(uri, options.into_kafka_connect_options())
                        .await
                        .map_err(map_err)?,
                ),
                "stdio" => SeaStreamerInner::Stdio(
                    StdioStreamer::connect(uri, options.into_stdio_connect_options())
                        .await
                        .map_err(map_err)?,
                ),
                _ => {
                    return Err(StreamErr::Connect(format!("unknown protocol `{protocol}`")));
                }
            },
            None => {
                return Err(StreamErr::Connect("protocol not set".to_owned()));
            }
        };
        Ok(SeaStreamer { backend })
    }

    /// It will flush all producers
    async fn disconnect(self) -> SeaResult<()> {
        match self.backend {
            SeaStreamerInner::Kafka(i) => i.disconnect().await.map_err(map_err),
            SeaStreamerInner::Stdio(i) => i.disconnect().await.map_err(map_err),
        }
    }

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> SeaResult<Self::Producer> {
        let backend = match &self.backend {
            SeaStreamerInner::Kafka(i) => SeaProducerBackend::Kafka(
                i.create_generic_producer(options.into_kafka_producer_options())
                    .await
                    .map_err(map_err)?,
            ),
            SeaStreamerInner::Stdio(i) => SeaProducerBackend::Stdio(
                i.create_generic_producer(options.into_stdio_producer_options())
                    .await
                    .map_err(map_err)?,
            ),
        };
        Ok(SeaProducer { backend })
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> SeaResult<Self::Consumer> {
        let backend = match &self.backend {
            SeaStreamerInner::Kafka(i) => SeaConsumerBackend::Kafka(
                i.create_consumer(streams, options.into_kafka_consumer_options())
                    .await
                    .map_err(map_err)?,
            ),
            SeaStreamerInner::Stdio(i) => SeaConsumerBackend::Stdio(
                i.create_consumer(streams, options.into_stdio_consumer_options())
                    .await
                    .map_err(map_err)?,
            ),
        };
        Ok(SeaConsumer { backend })
    }
}
