use std::str::FromStr;

use crate::{
    ConnectOptions, Consumer, ConsumerOptions, Producer, ProducerOptions, StreamKey, StreamResult,
};
use async_trait::async_trait;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamerUri {
    protocol: Option<String>,
    nodes: Vec<Url>,
}

#[async_trait]
pub trait Streamer: Sized {
    type Error: std::error::Error;
    type Producer: Producer<Error = Self::Error>;
    type Consumer: Consumer<Error = Self::Error>;
    type ConnectOptions: ConnectOptions;
    type ConsumerOptions: ConsumerOptions;
    type ProducerOptions: ProducerOptions;

    async fn connect(
        streamer: StreamerUri,
        options: Self::ConnectOptions,
    ) -> StreamResult<Self, Self::Error>;

    async fn disconnect(self) -> StreamResult<(), Self::Error>;

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer, Self::Error>;

    async fn create_producer(
        &self,
        stream: StreamKey,
        options: Self::ProducerOptions,
    ) -> StreamResult<Self::Producer, Self::Error> {
        let mut producer = self.create_generic_producer(options).await?;
        producer.anchor(stream)?;
        Ok(producer)
    }

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

impl FromStr for StreamerUri {
    type Err = url::ParseError;

    fn from_str(mut urls: &str) -> Result<Self, Self::Err> {
        let mut protocol = None;
        if let Some((front, remaining)) = urls.split_once("://") {
            protocol = Some(front);
            urls = remaining;
        }
        let urls: Vec<_> = urls
            .split(',')
            .filter(|x| !x.is_empty())
            .map(FromStr::from_str)
            .collect();
        Ok(Self {
            protocol: protocol.map(|s| s.to_owned()),
            nodes: urls.into_iter().collect::<Result<Vec<_>, Self::Err>>()?,
        })
    }
}
