use std::{sync::Arc, time::Duration};

use crate::{
    create_consumer, create_manager, create_producer, RedisCluster, RedisConsumer,
    RedisConsumerOptions, RedisErr, RedisManager, RedisManagerOptions, RedisProducer,
    RedisProducerOptions, RedisResult, MSG, REDIS_PORT,
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
    timestamp_format: TimestampFormat,
    message_field: MessageField,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct MessageField(pub &'static str);

impl Default for MessageField {
    fn default() -> Self {
        Self(MSG)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub enum TimestampFormat {
    #[default]
    UnixTimestampMillis,
    #[cfg(feature = "nanosecond-timestamp")]
    UnixTimestampNanos,
}

impl Streamer for RedisStreamer {
    type Error = RedisErr;
    type Producer = RedisProducer;
    type Consumer = RedisConsumer;
    type ConnectOptions = RedisConnectOptions;
    type ConsumerOptions = RedisConsumerOptions;
    type ProducerOptions = RedisProducerOptions;

    async fn connect(uri: StreamerUri, options: Self::ConnectOptions) -> RedisResult<Self> {
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
        mut options: Self::ProducerOptions,
    ) -> RedisResult<Self::Producer> {
        options.timestamp_format = self.options.timestamp_format;
        options.message_field = self.options.message_field;
        let cluster = RedisCluster::new(self.uri.clone(), self.options.clone())?;
        create_producer(cluster, options).await
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        mut options: Self::ConsumerOptions,
    ) -> RedisResult<Self::Consumer> {
        options.timestamp_format = self.options.timestamp_format;
        options.message_field = self.options.message_field;
        let cluster = RedisCluster::new(self.uri.clone(), self.options.clone())?;
        create_consumer(cluster, options, streams.to_vec()).await
    }
}

impl RedisStreamer {
    pub async fn create_manager(&self) -> RedisResult<RedisManager> {
        let mut options = RedisManagerOptions::default();
        options.timestamp_format = self.options.timestamp_format;
        options.message_field = self.options.message_field;
        let cluster = RedisCluster::new(self.uri.clone(), self.options.clone())?;
        create_manager(cluster, options).await
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

    pub fn timestamp_format(&self) -> TimestampFormat {
        self.timestamp_format
    }
    /// Set timestamp format. i.e. the default timestamp format is milliseconds,
    /// which is recommended by Redis.
    pub fn set_timestamp_format(&mut self, fmt: TimestampFormat) -> &mut Self {
        self.timestamp_format = fmt;
        self
    }

    pub fn message_field(&self) -> &'static str {
        self.message_field.0
    }
    /// The field used to hold the message. Defaults to [`crate::MSG`].
    pub fn set_message_field(&mut self, field: &'static str) -> &mut Self {
        self.message_field = MessageField(field);
        self
    }
}
