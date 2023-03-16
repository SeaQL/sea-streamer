use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::{
    map_err, NodeId, RedisConnectOptions, RedisErr, RedisResult, DEFAULT_TIMEOUT, REDIS_PORT,
};
use redis::{ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use sea_streamer_runtime::{sleep, timeout};
use sea_streamer_types::{ConnectOptions, StreamErr};

#[derive(Debug)]
/// A wrapped [`redis::aio::Connection`] that can auto-reconnect
pub struct Connection {
    node: NodeId,
    options: Arc<RedisConnectOptions>,
    state: State,
}

enum State {
    Alive(redis::aio::Connection),
    Reconnecting { delay: u32 },
    Dead,
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alive(_) => f.debug_tuple("Alive").finish(),
            Self::Reconnecting { delay } => f.debug_tuple("Reconnecting").field(delay).finish(),
            Self::Dead => f.debug_tuple("Dead").finish(),
        }
    }
}

impl Connection {
    /// Create a connection; return error if failed.
    pub async fn create(node: NodeId, options: Arc<RedisConnectOptions>) -> RedisResult<Self> {
        let conn = create_connection(node.clone(), options.clone()).await?;
        log::debug!("Opened connection to {node}");
        Ok(Self {
            node,
            options,
            state: State::Alive(conn),
        })
    }

    /// Create a connection; but retry later if failed.
    pub async fn create_or_reconnect(
        node: NodeId,
        options: Arc<RedisConnectOptions>,
    ) -> RedisResult<Self> {
        if let Ok(conn) = create_connection(node.clone(), options.clone()).await {
            Ok(Self {
                node,
                options,
                state: State::Alive(conn),
            })
        } else {
            Ok(Self {
                node,
                options,
                state: State::Reconnecting { delay: 1 },
            })
        }
    }

    /// Drop the connection and reconnect *later*.
    pub fn reconnect(&mut self) {
        self.state = State::Reconnecting { delay: 1 };
    }

    /// Get a mutable connection, will wait and retry a few times until dead.
    pub async fn get(&mut self) -> RedisResult<&mut redis::aio::Connection> {
        match &mut self.state {
            State::Alive(_) | State::Dead => (),
            State::Reconnecting { delay } => {
                assert!(*delay > 0);
                sleep(Duration::from_secs(*delay as u64)).await;
                match create_connection(self.node.clone(), self.options.clone()).await {
                    Ok(conn) => {
                        self.state = State::Alive(conn);
                    }
                    Err(_) => {
                        if *delay > 60 {
                            self.state = State::Dead;
                        } else {
                            self.state = State::Reconnecting { delay: *delay * 2 };
                        }
                    }
                }
            }
        }
        self.try_get()
    }

    /// Get a mutable connection, only if it is alive.
    pub fn try_get(&mut self) -> RedisResult<&mut redis::aio::Connection> {
        match &mut self.state {
            State::Alive(conn) => Ok(conn),
            State::Dead => Err(StreamErr::Connect(format!(
                "Connection to {} is dead.",
                self.node
            ))),
            State::Reconnecting { .. } => Err(StreamErr::Backend(RedisErr::TryAgain(format!(
                "Reconnecting to {}",
                self.node
            )))),
        }
    }

    pub fn protocol(&self) -> &str {
        self.node.scheme()
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node
    }
}

async fn create_connection(
    url: NodeId,
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
