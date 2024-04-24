use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{Connection, RedisConnectOptions, RedisResult};
use sea_streamer_types::{export::url::Url, StreamErr, StreamUrlErr, StreamerUri};

/// ID of a node in a Redis Cluster.
pub type NodeId = Url;

#[derive(Debug)]
/// A set of connections maintained to a Redis Cluster with key cache.
pub struct RedisCluster {
    pub(crate) cluster: StreamerUri,
    pub(crate) options: Arc<RedisConnectOptions>,
    pub(crate) conn: HashMap<NodeId, Connection>,
    pub(crate) keys: HashMap<String, NodeId>,
}

impl RedisCluster {
    /// Nothing happens until you call connect
    pub fn new(cluster: StreamerUri, options: Arc<RedisConnectOptions>) -> RedisResult<Self> {
        if cluster.nodes().is_empty() {
            return Err(StreamErr::StreamUrlErr(StreamUrlErr::ZeroNode));
        }
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
            let conn = Connection::create(node.clone(), self.options.clone()).await?;
            self.conn.insert(node.clone(), conn);
        }
        Ok(())
    }

    /// An error has occured on the connection. Attempt to reconnect *later*.
    pub fn reconnect(&mut self, node: &NodeId) -> RedisResult<()> {
        if let Some(inner) = self.conn.get_mut(node) {
            inner.reconnect();
        }
        Ok(())
    }

    #[inline]
    /// Get the cached node for this key. There is no guarantee that the key assignment is right.
    pub fn node_for(&self, key: &str) -> &NodeId {
        Self::get_node_for(&self.keys, &self.cluster, key)
    }

    fn get_node_for<'a>(
        keys: &'a HashMap<String, NodeId>,
        cluster: &'a StreamerUri,
        key: &str,
    ) -> &'a NodeId {
        if let Some(node) = keys.get(key) {
            node
        } else {
            cluster.nodes().first().expect("Should not be empty")
        }
    }

    /// Indicate that the particular key has been moved to a different node in the cluster.
    pub fn moved(&mut self, key: &str, node: NodeId) {
        if let Some(inner) = self.keys.get_mut(key) {
            *inner = node;
        } else {
            self.keys.insert(key.to_owned(), node);
        }
    }

    /// Get any available connection to the cluster
    pub fn get_any(&mut self) -> RedisResult<(&NodeId, &mut redis::aio::MultiplexedConnection)> {
        for (node, inner) in self.conn.iter_mut() {
            if let Ok(conn) = inner.try_get() {
                return Ok((node, conn));
            }
        }
        Err(StreamErr::Connect("No open connections".to_owned()))
    }

    #[inline]
    /// Get a connection to the specific node, will wait and retry a few times until dead.
    pub async fn get(
        &mut self,
        node: &NodeId,
    ) -> RedisResult<&mut redis::aio::MultiplexedConnection> {
        Self::get_connection(&mut self.conn, &self.options, node).await
    }

    #[inline]
    /// Get a connection that is assigned with the specific key, will wait and retry a few times until dead.
    /// There is no guarantee that the key assignment is right.
    pub async fn get_connection_for(
        &mut self,
        key: &str,
    ) -> RedisResult<(&NodeId, &mut redis::aio::MultiplexedConnection)> {
        let node = Self::get_node_for(&self.keys, &self.cluster, key);
        Ok((
            node,
            Self::get_connection(&mut self.conn, &self.options, node).await?,
        ))
    }

    async fn get_connection<'a>(
        conn: &'a mut HashMap<NodeId, Connection>,
        options: &Arc<RedisConnectOptions>,
        node: &NodeId,
    ) -> RedisResult<&'a mut redis::aio::MultiplexedConnection> {
        assert!(!node.scheme().is_empty(), "Must have protocol");
        assert!(node.host_str().is_some(), "Must have host");
        assert!(node.port().is_some(), "Must have port");
        if let Some(state) = conn.get_mut(node) {
            state.get().await.ok();
        } else {
            conn.insert(
                node.clone(),
                Connection::create_or_reconnect(node.clone(), options.clone()).await?,
            );
        }
        conn.get_mut(node).expect("Must exist").try_get()
    }
}
