use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use crate::{map_err, RedisConnectOptions, RedisErr, RedisResult, DEFAULT_TIMEOUT, REDIS_PORT};
use redis::{ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use sea_streamer_runtime::{sleep, timeout};
use sea_streamer_types::{export::url::Url, ConnectOptions, StreamErr, StreamUrlErr, StreamerUri};

#[derive(Debug)]
pub struct RedisCluster {
    cluster: StreamerUri,
    options: Arc<RedisConnectOptions>,
    conn: HashMap<Url, Connection>,
    keys: HashMap<String, Url>,
}

enum Connection {
    Alive(redis::aio::Connection),
    Reconnecting { delay: u32 },
    Dead,
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alive(_) => f.debug_tuple("Alive").finish(),
            Self::Reconnecting { delay } => f.debug_tuple("Reconnecting").field(delay).finish(),
            Self::Dead => f.debug_tuple("Dead").finish(),
        }
    }
}

impl RedisCluster {
    /// Nothing happens until you call connect
    pub fn new(cluster: StreamerUri, options: Arc<RedisConnectOptions>) -> RedisResult<Self> {
        if cluster.protocol().is_none() {
            return Err(StreamErr::StreamUrlErr(StreamUrlErr::ProtocolRequired));
        }
        Ok(Self {
            cluster,
            options,
            conn: Default::default(),
            keys: Default::default(),
        })
    }

    pub fn protocol(&self) -> Option<&str> {
        self.cluster.protocol()
    }

    /// Will drop all existing connections. This method returns OK only if it can connect to all nodes.
    pub async fn reconnect_all(&mut self) -> RedisResult<()> {
        self.conn = Default::default();
        for node in self.cluster.nodes() {
            let conn = create_connection(node.clone(), self.options.clone()).await?;
            self.conn.insert(node.clone(), Connection::Alive(conn));
        }
        Ok(())
    }

    /// An error has occured on the connection. Attempt to reconnect *later*.
    pub fn reconnect(&mut self, node: &Url) -> RedisResult<()> {
        if let Some(state) = self.conn.get_mut(node) {
            *state = Connection::Reconnecting { delay: 1 };
        }
        Ok(())
    }

    #[inline]
    /// Get the cached node for this key. There is no guarantee that the key assignment is right.
    pub fn node_for(&self, key: &str) -> &Url {
        Self::get_node_for(&self.keys, &self.cluster, key)
    }

    fn get_node_for<'a>(
        keys: &'a HashMap<String, Url>,
        cluster: &'a StreamerUri,
        key: &str,
    ) -> &'a Url {
        if let Some(node) = keys.get(key) {
            node
        } else {
            cluster.nodes().first().expect("Should not be empty")
        }
    }

    /// Indicate that the particular key has been moved.
    pub fn moved(&mut self, key: &str, node: Url) {
        if let Some(state) = self.keys.get_mut(key) {
            *state = node;
        } else {
            self.keys.insert(key.to_owned(), node);
        }
    }

    #[inline]
    /// Get a connection to the specific node, will wait and retry a few times until dead.
    pub async fn get(&mut self, node: &Url) -> RedisResult<&mut redis::aio::Connection> {
        Self::get_connection(&mut self.conn, &self.options, node).await
    }

    /// Get any available connection to the cluster
    pub fn get_any(&mut self) -> RedisResult<&mut redis::aio::Connection> {
        for state in self.conn.values_mut() {
            if let Connection::Alive(conn) = state {
                return Ok(conn);
            }
        }
        Err(StreamErr::Connect("No open connections".to_owned()))
    }

    #[inline]
    /// Get a connection that is assigned with the specific key, will wait and retry a few times until dead.
    /// There is no guarantee that the key assignment is right.
    pub async fn get_connection_for(
        &mut self,
        key: &str,
    ) -> RedisResult<(&Url, &mut redis::aio::Connection)> {
        let node = Self::get_node_for(&self.keys, &self.cluster, key);
        Ok((
            node,
            Self::get_connection(&mut self.conn, &self.options, node).await?,
        ))
    }

    async fn get_connection<'a>(
        conn: &'a mut HashMap<Url, Connection>,
        options: &Arc<RedisConnectOptions>,
        node: &Url,
    ) -> RedisResult<&'a mut redis::aio::Connection> {
        assert!(!node.scheme().is_empty(), "Must have protocol");
        assert!(node.host_str().is_some(), "Must have host");
        assert!(node.port().is_some(), "Must have port");
        if let Some(state) = conn.get_mut(node) {
            match state {
                Connection::Alive(_) | Connection::Dead => (),
                Connection::Reconnecting { delay } => {
                    assert!(*delay > 0);
                    sleep(Duration::from_secs(*delay as u64)).await;
                    match create_connection(node.clone(), options.clone()).await {
                        Ok(conn) => {
                            *state = Connection::Alive(conn);
                        }
                        Err(_) => {
                            if *delay > 60 {
                                *state = Connection::Dead;
                            } else {
                                *state = Connection::Reconnecting { delay: *delay * 2 };
                            }
                        }
                    }
                }
            }
        } else {
            let state = match create_connection(node.clone(), options.clone()).await {
                Ok(conn) => Connection::Alive(conn),
                Err(_) => Connection::Reconnecting { delay: 1 },
            };
            conn.insert(node.clone(), state);
        }
        if let Some(state) = conn.get_mut(node) {
            match state {
                Connection::Alive(conn) => Ok(conn),
                Connection::Dead => Err(StreamErr::Connect(format!(
                    "Connection to {node:?} is dead."
                ))),
                Connection::Reconnecting { .. } => Err(StreamErr::Backend(RedisErr::TryAgain(
                    format!("Reconnecting to {node:?}"),
                ))),
            }
        } else {
            unreachable!("Key must exist")
        }
    }
}

async fn create_connection(
    url: Url,
    options: Arc<RedisConnectOptions>,
) -> RedisResult<redis::aio::Connection> {
    let host = if let Some(host) = url.host_str() {
        host.to_owned()
    } else {
        return Err(StreamErr::Connect("Host empty".to_owned()));
    };
    let port = url.port().unwrap_or(REDIS_PORT);
    let conn = ConnectionInfo {
        addr: match url.scheme() {
            "redis" => ConnectionAddr::Tcp(host, port),
            "rediss" => ConnectionAddr::TcpTls {
                host,
                port,
                insecure: options.disable_hostname_verification(),
            },
            "" => return Err(StreamErr::Connect("protocol not set".to_owned())),
            protocol => return Err(StreamErr::Connect(format!("unknown protocol `{protocol}`"))),
        },
        redis: RedisConnectionInfo {
            db: options.db() as i64,
            username: options.username().map(|s| s.to_owned()),
            password: options.password().map(|s| s.to_owned()),
        },
    };
    let client = redis::Client::open(conn).map_err(map_err)?;
    // I wish we could do `.await_timeout(d)` some day
    match timeout(
        options.timeout().unwrap_or(DEFAULT_TIMEOUT),
        client.get_async_connection(),
    )
    .await
    {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(err)) => Err(map_err(err)),
        Err(_) => Err(StreamErr::Connect("Connection timeout".to_owned())),
    }
}
