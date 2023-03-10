use std::time::Duration;

use crate::{
    create_producer, map_err, RedisConsumer, RedisConsumerOptions, RedisErr, RedisProducer,
    RedisProducerOptions, RedisResult, REDIS_PORT,
};
use redis::{ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use sea_streamer_runtime::timeout;
use sea_streamer_types::{
    export::async_trait, ConnectOptions, StreamErr, StreamKey, Streamer, StreamerUri,
};

#[derive(Debug, Clone)]
pub struct RedisStreamer {
    uri: StreamerUri,
    options: RedisConnectOptions,
}

#[derive(Debug, Default, Clone)]
pub struct RedisConnectOptions {
    db: u32,
    username: Option<String>,
    password: Option<String>,
    timeout: Option<Duration>,
    disable_hostname_verification: bool,
}

#[async_trait]
impl Streamer for RedisStreamer {
    type Error = RedisErr;
    type Producer = RedisProducer;
    type Consumer = RedisConsumer;
    type ConnectOptions = RedisConnectOptions;
    type ConsumerOptions = RedisConsumerOptions;
    type ProducerOptions = RedisProducerOptions;

    async fn connect(uri: StreamerUri, options: Self::ConnectOptions) -> RedisResult<Self> {
        Ok(RedisStreamer { uri, options })
    }

    async fn disconnect(self) -> RedisResult<()> {
        todo!()
    }

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> RedisResult<Self::Producer> {
        create_producer(self.create_connection().await?, options).await
    }

    async fn create_consumer(
        &self,
        _streams: &[StreamKey],
        _options: Self::ConsumerOptions,
    ) -> RedisResult<Self::Consumer> {
        todo!()
    }
}

impl RedisStreamer {
    async fn create_connection(&self) -> RedisResult<redis::aio::Connection> {
        let nodes = self.uri.nodes();
        if nodes.is_empty() {
            return Err(StreamErr::Connect("There are no nodes".to_owned()));
        }
        let url = nodes.first().unwrap();
        let host = if let Some(host) = url.host_str() {
            host.to_owned()
        } else {
            return Err(StreamErr::Connect("Host empty".to_owned()));
        };
        let port = url.port().unwrap_or(REDIS_PORT);
        let conn = ConnectionInfo {
            addr: match self.uri.protocol() {
                Some("redis") => ConnectionAddr::Tcp(host, port),
                Some("rediss") => ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: self.options.disable_hostname_verification,
                },
                Some(protocol) => {
                    return Err(StreamErr::Connect(format!("unknown protocol `{protocol}`")))
                }
                None => return Err(StreamErr::Connect("protocol not set".to_owned())),
            },
            redis: RedisConnectionInfo {
                db: self.options.db as i64,
                username: self.options.username.clone(),
                password: self.options.password.clone(),
            },
        };
        let client = redis::Client::open(conn).map_err(map_err)?;
        if let Ok(dur) = self.options.timeout() {
            // I wish we could do `.await_timeout(d)` some day
            match timeout(dur, client.get_async_connection()).await {
                Ok(Ok(conn)) => Ok(conn),
                Ok(Err(err)) => Err(map_err(err)),
                Err(_) => Err(StreamErr::Connect("Connection timeout".to_owned())),
            }
        } else {
            client.get_async_connection().await.map_err(map_err)
        }
    }
}

impl ConnectOptions for RedisConnectOptions {
    type Error = RedisErr;

    fn timeout(&self) -> RedisResult<Duration> {
        self.timeout.ok_or(StreamErr::TimeoutNotSet)
    }

    /// Timeout for network requests. If unset, it will never timeout.
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
