use std::str::FromStr;

use crate::{
    ConnectOptions, Consumer, ConsumerOptions, Producer, ProducerOptions, StreamKey, StreamResult,
    StreamUrlErr,
};
use async_trait::async_trait;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// URI of Streaming Server. If this is a cluster, there can be multiple nodes.
pub struct StreamerUri {
    protocol: Option<String>,
    nodes: Vec<Url>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Streamer URI with stream key(s).
///
/// Examples:
///
/// ```ignore
/// stdio://
/// stdio:///stream_a,stream_b
/// redis://localhost/
/// kafka://node-a:1234,node-b:1234/stream_a,stream_b
/// ```
pub struct StreamUrl {
    streamer: StreamerUri,
    streams: Vec<StreamKey>,
}

#[async_trait]
/// Common interface of streamer clients.
pub trait Streamer: Sized {
    type Error: std::error::Error;
    type Producer: Producer<Error = Self::Error>;
    type Consumer: Consumer<Error = Self::Error>;
    type ConnectOptions: ConnectOptions;
    type ConsumerOptions: ConsumerOptions;
    type ProducerOptions: ProducerOptions;

    /// Establish a connection to the streaming server.
    async fn connect(
        streamer: StreamerUri,
        options: Self::ConnectOptions,
    ) -> StreamResult<Self, Self::Error>;

    /// Flush and disconnect from the streaming server.
    async fn disconnect(self) -> StreamResult<(), Self::Error>;

    /// Create a producer that can stream to any stream key.
    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer, Self::Error>;

    /// Create a producer that streams to the specified stream.
    async fn create_producer(
        &self,
        stream: StreamKey,
        options: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer, Self::Error> {
        let mut producer = self.create_generic_producer(options).await?;
        producer.anchor(stream)?;
        Ok(producer)
    }

    /// Create a consumer subscribing to the specified streams.
    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> StreamResult<Self::Consumer, Self::Error>;
}

impl StreamerUri {
    pub fn zero() -> Self {
        Self {
            protocol: None,
            nodes: Vec::new(),
        }
    }

    pub fn one(url: Url) -> Self {
        Self {
            protocol: None,
            nodes: vec![url],
        }
    }

    pub fn many(urls: impl Iterator<Item = Url>) -> Self {
        Self {
            protocol: None,
            nodes: urls.collect(),
        }
    }

    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    pub fn nodes(&self) -> &[Url] {
        &self.nodes
    }
}

impl StreamUrl {
    pub fn streamer(&self) -> StreamerUri {
        self.streamer.to_owned()
    }

    pub fn streamer_ref(&self) -> &StreamerUri {
        &self.streamer
    }

    pub fn stream_keys(&self) -> &[StreamKey] {
        &self.streams
    }

    pub fn stream_key(&self) -> Result<StreamKey, StreamUrlErr> {
        if self.streams.len() == 1 {
            Ok(self.streams[0].to_owned())
        } else {
            Err(StreamUrlErr::NotOneStreamKey)
        }
    }
}

impl FromStr for StreamUrl {
    type Err = StreamUrlErr;

    fn from_str(mut urls: &str) -> Result<Self, Self::Err> {
        let mut protocol = None;
        let mut streams = None;
        if let Some((front, remaining)) = urls.split_once("://") {
            protocol = Some(front);
            urls = remaining;
        }
        if let Some((front, remaining)) = urls.split_once('/') {
            urls = front;
            streams = Some(remaining);
        }
        let urls: Vec<_> = urls
            .split(',')
            .filter(|x| !x.is_empty())
            .map(|s| {
                if let Some(protocol) = protocol {
                    FromStr::from_str(format!("{protocol}://{s}").as_str())
                } else {
                    FromStr::from_str(s)
                }
                .map_err(Into::into)
            })
            .collect();

        Ok(StreamUrl {
            streamer: StreamerUri {
                protocol: protocol.map(|s| s.to_owned()),
                nodes: urls.into_iter().collect::<Result<Vec<_>, Self::Err>>()?,
            },
            streams: match streams {
                None => Default::default(),
                Some(streams) => streams
                    .split(',')
                    .filter(|x| !x.is_empty())
                    .map(|n| StreamKey::new(n).map_err(Into::into))
                    .collect::<Result<Vec<StreamKey>, StreamUrlErr>>()?,
            },
        })
    }
}

impl FromStr for StreamerUri {
    type Err = StreamUrlErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(StreamUrl::from_str(s)?.streamer)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_stream_url() {
        let stream_keys = vec![StreamKey::new("a").unwrap(), StreamKey::new("b").unwrap()];

        let streamer: StreamerUri = "sea-ql.org:1234".parse().unwrap();
        assert_eq!(streamer.protocol(), None);
        assert_eq!(streamer.nodes(), &["sea-ql.org:1234".parse().unwrap()]);

        let stream: StreamUrl = "proto://sea-ql.org:1234".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("proto"));
        assert_eq!(
            stream.streamer.nodes(),
            &["proto://sea-ql.org:1234".parse().unwrap()]
        );
        assert_eq!(stream.stream_keys(), &[]);

        let stream: StreamUrl = "proto://sea-ql.org:1234/".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("proto"));
        assert_eq!(
            stream.streamer.nodes(),
            &["proto://sea-ql.org:1234".parse().unwrap()]
        );
        assert_eq!(stream.stream_keys(), &[]);

        let stream: StreamUrl = "proto://sea-ql.org:1234/stream".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("proto"));
        assert_eq!(
            stream.streamer.nodes(),
            &["proto://sea-ql.org:1234".parse().unwrap()]
        );
        assert_eq!(stream.stream_keys(), &[StreamKey::new("stream").unwrap()]);

        let stream: StreamUrl = "proto://sea-ql.org:1234/a,b".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("proto"));
        assert_eq!(
            stream.streamer.nodes(),
            &["proto://sea-ql.org:1234".parse().unwrap()]
        );
        assert_eq!(stream.stream_keys(), &stream_keys);

        let nodes = [
            "kafka://node-a:1234".parse().unwrap(),
            "kafka://node-b:1234".parse().unwrap(),
        ];
        let stream: StreamUrl = "kafka://node-a:1234,node-b:1234/a,b".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("kafka"));
        assert_eq!(stream.streamer.nodes(), &nodes);
        assert_eq!(stream.stream_keys(), &stream_keys);

        let stream: StreamUrl = "stdio://".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("stdio"));
        assert_eq!(stream.streamer.nodes(), &[]);
        assert_eq!(stream.stream_keys(), &[]);

        let stream: StreamUrl = "redis://localhost/".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("redis"));
        assert_eq!(
            stream.streamer.nodes(),
            &["redis://localhost".parse().unwrap()]
        );
        assert_eq!(stream.stream_keys(), &[]);

        let stream: StreamUrl = "stdio:///a,b".parse().unwrap();
        assert_eq!(stream.streamer.protocol(), Some("stdio"));
        assert_eq!(stream.streamer.nodes(), &[]);
        assert_eq!(stream.stream_keys(), &stream_keys);
    }

    #[test]
    fn test_parse_stream_url_err() {
        use crate::StreamKeyErr;

        assert!(matches!(
            "proto://sea-ql.org:1234/stream?".parse::<StreamUrl>(),
            Err(StreamUrlErr::StreamKeyErr(StreamKeyErr::InvalidStreamKey))
        ));
    }
}
