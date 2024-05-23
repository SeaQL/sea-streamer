#[cfg(feature = "backend-file")]
use sea_streamer_file::FileStreamer;
#[cfg(feature = "backend-kafka")]
use sea_streamer_kafka::KafkaStreamer;
#[cfg(feature = "backend-redis")]
use sea_streamer_redis::RedisStreamer;
#[cfg(feature = "backend-stdio")]
use sea_streamer_stdio::StdioStreamer;

use sea_streamer_types::{StreamErr, StreamKey, Streamer, StreamerUri};

use crate::{
    map_err, Backend, BackendErr, SeaConnectOptions, SeaConsumer, SeaConsumerBackend,
    SeaConsumerOptions, SeaProducer, SeaProducerBackend, SeaProducerOptions, SeaResult,
    SeaStreamerBackend,
};

#[derive(Debug)]
/// `sea-streamer-socket` concrete type of Streamer.
pub struct SeaStreamer {
    backend: SeaStreamerInner,
}

#[derive(Debug, Clone)]
pub(crate) enum SeaStreamerInner {
    #[cfg(feature = "backend-kafka")]
    Kafka(KafkaStreamer),
    #[cfg(feature = "backend-redis")]
    Redis(RedisStreamer),
    #[cfg(feature = "backend-stdio")]
    Stdio(StdioStreamer),
    #[cfg(feature = "backend-file")]
    File(FileStreamer),
}

#[cfg(feature = "backend-kafka")]
impl From<KafkaStreamer> for SeaStreamer {
    fn from(i: KafkaStreamer) -> Self {
        Self {
            backend: SeaStreamerInner::Kafka(i),
        }
    }
}

#[cfg(feature = "backend-redis")]
impl From<RedisStreamer> for SeaStreamer {
    fn from(i: RedisStreamer) -> Self {
        Self {
            backend: SeaStreamerInner::Redis(i),
        }
    }
}

#[cfg(feature = "backend-stdio")]
impl From<StdioStreamer> for SeaStreamer {
    fn from(i: StdioStreamer) -> Self {
        Self {
            backend: SeaStreamerInner::Stdio(i),
        }
    }
}

#[cfg(feature = "backend-file")]
impl From<FileStreamer> for SeaStreamer {
    fn from(i: FileStreamer) -> Self {
        Self {
            backend: SeaStreamerInner::File(i),
        }
    }
}

impl SeaStreamerBackend for SeaStreamer {
    #[cfg(feature = "backend-kafka")]
    type Kafka = KafkaStreamer;
    #[cfg(feature = "backend-redis")]
    type Redis = RedisStreamer;
    #[cfg(feature = "backend-stdio")]
    type Stdio = StdioStreamer;
    #[cfg(feature = "backend-file")]
    type File = FileStreamer;

    fn backend(&self) -> Backend {
        match self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaStreamerInner::Kafka(_) => Backend::Kafka,
            #[cfg(feature = "backend-redis")]
            SeaStreamerInner::Redis(_) => Backend::Redis,
            #[cfg(feature = "backend-stdio")]
            SeaStreamerInner::Stdio(_) => Backend::Stdio,
            #[cfg(feature = "backend-file")]
            SeaStreamerInner::File(_) => Backend::File,
        }
    }

    #[cfg(feature = "backend-kafka")]
    fn get_kafka(&mut self) -> Option<&mut Self::Kafka> {
        match &mut self.backend {
            SeaStreamerInner::Kafka(s) => Some(s),
            #[cfg(feature = "backend-redis")]
            SeaStreamerInner::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            SeaStreamerInner::Stdio(_) => None,
            #[cfg(feature = "backend-file")]
            SeaStreamerInner::File(_) => None,
        }
    }

    #[cfg(feature = "backend-redis")]
    fn get_redis(&mut self) -> Option<&mut Self::Redis> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaStreamerInner::Kafka(_) => None,
            SeaStreamerInner::Redis(s) => Some(s),
            #[cfg(feature = "backend-stdio")]
            SeaStreamerInner::Stdio(_) => None,
            #[cfg(feature = "backend-file")]
            SeaStreamerInner::File(_) => None,
        }
    }

    #[cfg(feature = "backend-stdio")]
    fn get_stdio(&mut self) -> Option<&mut Self::Stdio> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaStreamerInner::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            SeaStreamerInner::Redis(_) => None,
            SeaStreamerInner::Stdio(s) => Some(s),
            #[cfg(feature = "backend-file")]
            SeaStreamerInner::File(_) => None,
        }
    }

    #[cfg(feature = "backend-file")]
    fn get_file(&mut self) -> Option<&mut Self::File> {
        match &mut self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaStreamerInner::Kafka(_) => None,
            #[cfg(feature = "backend-redis")]
            SeaStreamerInner::Redis(_) => None,
            #[cfg(feature = "backend-stdio")]
            SeaStreamerInner::Stdio(_) => None,
            SeaStreamerInner::File(s) => Some(s),
        }
    }
}

impl Streamer for SeaStreamer {
    type Error = BackendErr;
    type Producer = SeaProducer;
    type Consumer = SeaConsumer;
    type ConnectOptions = SeaConnectOptions;
    type ConsumerOptions = SeaConsumerOptions;
    type ProducerOptions = SeaProducerOptions;

    async fn connect(
        uri: impl Into<StreamerUri>,
        options: Self::ConnectOptions,
    ) -> SeaResult<Self> {
        let uri = uri.into();
        let backend = match uri.protocol() {
            Some(protocol) => match protocol {
                #[cfg(feature = "backend-kafka")]
                "kafka" => SeaStreamerInner::Kafka(
                    KafkaStreamer::connect(uri, options.into_kafka_connect_options())
                        .await
                        .map_err(map_err)?,
                ),
                #[cfg(feature = "backend-redis")]
                "redis" | "rediss" => SeaStreamerInner::Redis(
                    RedisStreamer::connect(uri, options.into_redis_connect_options())
                        .await
                        .map_err(map_err)?,
                ),
                #[cfg(feature = "backend-stdio")]
                "stdio" => SeaStreamerInner::Stdio(
                    StdioStreamer::connect(uri, options.into_stdio_connect_options())
                        .await
                        .map_err(map_err)?,
                ),
                #[cfg(feature = "backend-file")]
                "file" => SeaStreamerInner::File(
                    FileStreamer::connect(uri, options.into_file_connect_options())
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

    async fn disconnect(self) -> SeaResult<()> {
        match self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaStreamerInner::Kafka(i) => i.disconnect().await.map_err(map_err),
            #[cfg(feature = "backend-redis")]
            SeaStreamerInner::Redis(i) => i.disconnect().await.map_err(map_err),
            #[cfg(feature = "backend-stdio")]
            SeaStreamerInner::Stdio(i) => i.disconnect().await.map_err(map_err),
            #[cfg(feature = "backend-file")]
            SeaStreamerInner::File(i) => i.disconnect().await.map_err(map_err),
        }
    }

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> SeaResult<Self::Producer> {
        let backend = match &self.backend {
            #[cfg(feature = "backend-kafka")]
            SeaStreamerInner::Kafka(i) => SeaProducerBackend::Kafka(
                i.create_generic_producer(options.into_kafka_producer_options())
                    .await
                    .map_err(map_err)?,
            ),
            #[cfg(feature = "backend-redis")]
            SeaStreamerInner::Redis(i) => SeaProducerBackend::Redis(
                i.create_generic_producer(options.into_redis_producer_options())
                    .await
                    .map_err(map_err)?,
            ),
            #[cfg(feature = "backend-stdio")]
            SeaStreamerInner::Stdio(i) => SeaProducerBackend::Stdio(
                i.create_generic_producer(options.into_stdio_producer_options())
                    .await
                    .map_err(map_err)?,
            ),
            #[cfg(feature = "backend-file")]
            SeaStreamerInner::File(i) => SeaProducerBackend::File(
                i.create_generic_producer(options.into_file_producer_options())
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
            #[cfg(feature = "backend-kafka")]
            SeaStreamerInner::Kafka(i) => SeaConsumerBackend::Kafka(
                i.create_consumer(streams, options.into_kafka_consumer_options())
                    .await
                    .map_err(map_err)?,
            ),
            #[cfg(feature = "backend-redis")]
            SeaStreamerInner::Redis(i) => SeaConsumerBackend::Redis(
                i.create_consumer(streams, options.into_redis_consumer_options())
                    .await
                    .map_err(map_err)?,
            ),
            #[cfg(feature = "backend-stdio")]
            SeaStreamerInner::Stdio(i) => SeaConsumerBackend::Stdio(
                i.create_consumer(streams, options.into_stdio_consumer_options())
                    .await
                    .map_err(map_err)?,
            ),
            #[cfg(feature = "backend-file")]
            SeaStreamerInner::File(i) => SeaConsumerBackend::File(
                i.create_consumer(streams, options.into_file_consumer_options())
                    .await
                    .map_err(map_err)?,
            ),
        };
        Ok(SeaConsumer { backend })
    }
}
