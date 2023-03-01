use rdkafka::ClientConfig;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use sea_streamer_runtime::spawn_blocking;
use sea_streamer_types::{
    export::async_trait, runtime_error, ConnectOptions, ConsumerGroup, ConsumerMode,
    ConsumerOptions, StreamErr, StreamKey, Streamer, StreamerUri,
};

use crate::{
    cluster::cluster_uri, create_consumer, create_producer, host_id, impl_into_string,
    KafkaConsumer, KafkaConsumerOptions, KafkaErr, KafkaProducer, KafkaProducerOptions,
    KafkaResult,
};

#[derive(Debug, Clone)]
pub struct KafkaStreamer {
    uri: StreamerUri,
    producers: Arc<Mutex<Vec<KafkaProducer>>>,
    options: KafkaConnectOptions,
}

#[derive(Debug, Default, Clone)]
pub struct KafkaConnectOptions {
    timeout: Option<Duration>,
    custom_options: Vec<(String, String)>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum BaseOptionKey {
    BootstrapServers,
    SocketTimeout,
}

type AdminClient = rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>;

impl ConnectOptions for KafkaConnectOptions {
    type Error = KafkaErr;

    fn timeout(&self) -> KafkaResult<Duration> {
        self.timeout.ok_or(StreamErr::TimeoutNotSet)
    }

    /// Timeout for network requests. Default is 1 min
    fn set_timeout(&mut self, v: Duration) -> KafkaResult<&mut Self> {
        self.timeout = Some(v);
        Ok(self)
    }
}

impl KafkaConnectOptions {
    /// Add a custom option. If you have an option you frequently use,
    /// please consider open a PR and add it to above.
    pub fn add_custom_option<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.custom_options.push((key.into(), value.into()));
        self
    }
    pub fn custom_options(&self) -> impl Iterator<Item = (&str, &str)> {
        self.custom_options
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub(crate) fn make_client_config(&self, client_config: &mut ClientConfig) {
        if let Some(v) = self.timeout {
            client_config.set(BaseOptionKey::SocketTimeout, format!("{}", v.as_millis()));
        }
        for (key, value) in self.custom_options() {
            client_config.set(key, value);
        }
    }
}

impl BaseOptionKey {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BootstrapServers => "bootstrap.servers",
            Self::SocketTimeout => "socket.timeout.ms",
        }
    }
}

impl_into_string!(BaseOptionKey);

#[async_trait]
impl Streamer for KafkaStreamer {
    type Error = KafkaErr;
    type Producer = KafkaProducer;
    type Consumer = KafkaConsumer;
    type ConnectOptions = KafkaConnectOptions;
    type ConsumerOptions = KafkaConsumerOptions;
    type ProducerOptions = KafkaProducerOptions;

    async fn connect(uri: StreamerUri, options: Self::ConnectOptions) -> KafkaResult<Self> {
        let admin = create_admin(&uri, &options).map_err(StreamErr::Backend)?;
        let timeout = options.timeout().unwrap_or(Duration::from_secs(1));
        spawn_blocking(move || admin.inner().fetch_cluster_id(timeout))
            .await
            .map_err(runtime_error)?
            .ok_or(StreamErr::Connect(
                "Failed to connect to streamer".to_owned(),
            ))?;

        Ok(KafkaStreamer {
            uri,
            producers: Arc::new(Mutex::new(Vec::new())),
            options,
        })
    }

    /// It will flush all producers
    async fn disconnect(self) -> KafkaResult<()> {
        let producers: Vec<KafkaProducer> = {
            let mut mutex = self.producers.lock().expect("Failed to lock KafkaStreamer");
            mutex.drain(..).collect()
        };
        for producer in producers {
            spawn_blocking(move || producer.flush_sync(std::time::Duration::from_secs(60)))
                .await
                .map_err(runtime_error)??;
        }
        Ok(())
    }

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> KafkaResult<Self::Producer> {
        let producer =
            create_producer(&self.uri, &self.options, &options).map_err(StreamErr::Backend)?;
        {
            let mut producers = self.producers.lock().expect("Failed to lock KafkaStreamer");
            producers.push(producer.clone());
        }
        Ok(producer)
    }

    /// If ConsumerMode is RealTime, auto commit will be disabled and it always stream from latest. `group_id` must not be set.
    ///
    /// If ConsumerMode is Resumable, it will use a group id unique to this host:
    /// on a physical machine, it will use the mac address. Inside a docker container, it will use the container id.
    /// So when the process restarts, it will resume from last committed offset. `group_id` must not be set.
    ///
    /// If ConsumerMode is LoadBalanced, shards will be divided-and-assigned by the broker to consumers sharing the same `group_id`.
    /// `group_id` must already be set.
    ///
    /// If you need to override the HOST ID, you can set the ENV var `HOST_ID`.
    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        mut options: Self::ConsumerOptions,
    ) -> KafkaResult<Self::Consumer> {
        if streams.is_empty() {
            return Err(StreamErr::StreamKeyEmpty);
        }
        match options.mode()? {
            ConsumerMode::RealTime => {
                if options.group_id().is_some() {
                    return Err(StreamErr::ConsumerGroupIsSet);
                }
                // I don't want to use a randomly generated ID
                options.set_group_id(ConsumerGroup::new(format!(
                    "{}s{}",
                    host_id(),
                    std::process::id()
                )));
                options.set_session_timeout(std::time::Duration::from_secs(6)); // trying to set it as low as allowed
                options.set_enable_auto_commit(false); // shall not auto-commit
            }
            ConsumerMode::Resumable => {
                if options.group_id().is_some() {
                    return Err(StreamErr::ConsumerGroupIsSet);
                }
                options.set_group_id(ConsumerGroup::new(format!("{}r", host_id())));
                // try not to override user config
                if options.session_timeout().is_none() {
                    options.set_session_timeout(std::time::Duration::from_secs(6));
                }
                if options.enable_auto_commit().is_none() {
                    options.set_enable_auto_commit(true);
                }
            }
            ConsumerMode::LoadBalanced => {
                if options.group_id().is_none() {
                    return Err(StreamErr::ConsumerGroupNotSet);
                }
            }
        }

        Ok(
            create_consumer(&self.uri, &self.options, &options, streams.to_vec())
                .map_err(StreamErr::Backend)?,
        )
    }
}

fn create_admin(
    streamer: &StreamerUri,
    base_options: &KafkaConnectOptions,
) -> Result<AdminClient, KafkaErr> {
    let mut client_config = ClientConfig::new();
    client_config.set(BaseOptionKey::BootstrapServers, cluster_uri(streamer)?);
    base_options.make_client_config(&mut client_config);

    let client: AdminClient = client_config.create()?;

    Ok(client)
}
