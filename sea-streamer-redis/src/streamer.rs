use std::{sync::Arc, time::Duration};

use crate::{
    create_consumer, create_producer, RedisCluster, RedisConsumer, RedisConsumerOptions, RedisErr,
    RedisProducer, RedisProducerOptions, RedisResult, REDIS_PORT,
};
use sea_streamer_types::{
    ConnectOptions, StreamErr, StreamKey, StreamUrlErr, Streamer, StreamerUri,
};

#[derive(Debug, Clone)]
/// The Redis Streamer, from which you can create Producers and Consumers.
pub struct RedisStreamer {
    uri: StreamerUri,
    options: Arc<RedisConnectOptions>,
}

#[derive(Debug, Default, Clone)]
/// Options for connections, including credentials.
pub struct RedisConnectOptions {
    db: u32,
    username: Option<String>,
    password: Option<String>,
    timeout: Option<Duration>,
    enable_cluster: bool,
    disable_hostname_verification: bool,
}

impl Streamer for RedisStreamer {
    type Error = RedisErr;
    type Producer = RedisProducer;
    type Consumer = RedisConsumer;
    type ConnectOptions = RedisConnectOptions;
    type ConsumerOptions = RedisConsumerOptions;
    type ProducerOptions = RedisProducerOptions;

    async fn connect<S>(streamer: S, options: Self::ConnectOptions) -> RedisResult<Self>
    where
        S: Into<StreamerUri> + Send,
    {
        let uri = streamer.into();
        if uri.protocol().is_none() {
            return Err(StreamErr::StreamUrlErr(StreamUrlErr::ProtocolRequired));
        }
        let uri = StreamerUri::many(uri.into_nodes().map(|u| {
            // normalize uri
            format!(
                "{}://{}:{}",
                u.scheme(),
                u.host().expect("Should have host"),
                u.port().unwrap_or(REDIS_PORT)
            )
            .parse()
            .expect("Must not fail")
        }));
        let options = Arc::new(options);
        let mut cluster = RedisCluster::new(uri.clone(), options.clone())?;
        cluster.reconnect_all().await?;
        Ok(RedisStreamer { uri, options })
    }

    async fn disconnect(self) -> RedisResult<()> {
        Err(StreamErr::Backend(RedisErr::Unknown(
            "Not implemented".to_owned(),
        )))
    }

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> RedisResult<Self::Producer> {
        let cluster = RedisCluster::new(self.uri.clone(), self.options.clone())?;
        create_producer(cluster, options).await
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> RedisResult<Self::Consumer> {
        let cluster = RedisCluster::new(self.uri.clone(), self.options.clone())?;
        create_consumer(cluster, options, streams.to_vec()).await
    }
}

impl ConnectOptions for RedisConnectOptions {
    type Error = RedisErr;

    fn timeout(&self) -> RedisResult<Duration> {
        self.timeout.ok_or(StreamErr::TimeoutNotSet)
    }

    /// Timeout for network requests. Defaults to [`crate::DEFAULT_TIMEOUT`].
    fn set_timeout(&mut self, v: Duration) -> RedisResult<&mut Self> {
        self.timeout = Some(v);
        Ok(self)
    }
}

impl RedisConnectOptions {
    /// Defaults to 0.
    pub fn db(&self) -> u32 {
        self.db
    }
    pub fn set_db(&mut self, db: u32) -> &mut Self {
        self.db = db;
        self
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }
    pub fn set_username(&mut self, username: Option<String>) -> &mut Self {
        self.username = username;
        self
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
    pub fn set_password(&mut self, password: Option<String>) -> &mut Self {
        self.password = password;
        self
    }

    pub fn enable_cluster(&self) -> bool {
        self.enable_cluster
    }
    /// Enable support for Redis Cluster.
    pub fn set_enable_cluster(&mut self, bool: bool) -> &mut Self {
        self.enable_cluster = bool;
        self
    }

    pub fn disable_hostname_verification(&self) -> bool {
        self.disable_hostname_verification
    }
    /// # Warning
    ///
    /// Only relevant if TLS is enabled and connecting to `rediss://`.
    /// Trust self-signed certificates. This is insecure. Do not use in production.
    pub fn set_disable_hostname_verification(&mut self, bool: bool) -> &mut Self {
        self.disable_hostname_verification = bool;
        self
    }
}
