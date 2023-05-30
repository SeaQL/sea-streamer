use std::{os::unix::prelude::MetadataExt, time::Duration};

use crate::{
    consumer::new_consumer, format::Header, FileConsumer, FileErr, FileId, FileProducer, FileResult,
};
use sea_streamer_runtime::file::File;
use sea_streamer_types::{
    export::async_trait, ConnectOptions as ConnectOptionsTrait, ConsumerGroup, ConsumerMode,
    ConsumerOptions as ConsumerOptionsTrait, ProducerOptions as ProducerOptionsTrait, StreamErr,
    StreamKey, StreamUrlErr, Streamer as StreamerTrait, StreamerUri,
};

#[derive(Debug, Clone)]
pub struct FileStreamer {
    file_id: FileId,
}

#[derive(Debug, Default, Clone)]
pub struct FileConnectOptions {}

#[derive(Debug, Clone)]
pub struct FileConsumerOptions {
    mode: ConsumerMode,
    group: Option<ConsumerGroup>,
    auto_stream_reset: AutoStreamReset,
}

#[derive(Debug, Default, Clone)]
pub struct FileProducerOptions {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Where to start streaming from.
pub enum AutoStreamReset {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StreamMode {
    Live,
    Replay,
}

#[async_trait]
impl StreamerTrait for FileStreamer {
    type Error = FileErr;
    type Producer = FileProducer;
    type Consumer = FileConsumer;
    type ConnectOptions = FileConnectOptions;
    type ConsumerOptions = FileConsumerOptions;
    type ProducerOptions = FileProducerOptions;

    /// First check whether the file exists.
    async fn connect(uri: StreamerUri, _: Self::ConnectOptions) -> FileResult<Self> {
        // TODO
        if uri.nodes().is_empty() {
            return Err(StreamErr::StreamUrlErr(StreamUrlErr::ZeroNode));
        }
        let path = uri
            .nodes()
            .first()
            .unwrap()
            .as_str()
            .trim_start_matches("file://")
            .trim_end_matches('/');
        dbg!(path);
        Ok(Self {
            file_id: FileId::new(path),
        })
    }

    async fn disconnect(self) -> FileResult<()> {
        todo!()
    }

    async fn create_generic_producer(
        &self,
        _: Self::ProducerOptions,
    ) -> FileResult<Self::Producer> {
        todo!()
    }

    async fn create_consumer(
        &self,
        streams: &[StreamKey],
        options: Self::ConsumerOptions,
    ) -> FileResult<Self::Consumer> {
        match options.mode {
            ConsumerMode::RealTime => {
                if options.group.is_some() {
                    return Err(StreamErr::ConsumerGroupIsSet);
                }
            }
            ConsumerMode::Resumable => {
                return Err(StreamErr::Unsupported(
                    "File does not support Resumable".to_owned(),
                ))
            }
            ConsumerMode::LoadBalanced => {
                if options.group.is_none() {
                    return Err(StreamErr::ConsumerGroupNotSet);
                }
            }
        }
        let stream_pos = match options.auto_stream_reset {
            AutoStreamReset::Earliest => {
                let file = File::open(self.file_id.path())
                    .await
                    .map_err(FileErr::IoError)?;
                if file.metadata().await.map_err(FileErr::IoError)?.size() <= Header::size() as u64
                {
                    StreamMode::Live
                } else {
                    StreamMode::Replay
                }
            }
            AutoStreamReset::Latest => StreamMode::Live,
        };
        let consumer = new_consumer(
            self.file_id.clone(),
            stream_pos,
            options.group,
            streams.to_vec(),
        )
        .await?;
        Ok(consumer)
    }
}

impl ConnectOptionsTrait for FileConnectOptions {
    type Error = FileErr;

    fn timeout(&self) -> FileResult<Duration> {
        Err(StreamErr::TimeoutNotSet)
    }

    /// This parameter is ignored.
    fn set_timeout(&mut self, _: Duration) -> FileResult<&mut Self> {
        Ok(self)
    }
}

impl ConsumerOptionsTrait for FileConsumerOptions {
    type Error = FileErr;

    fn new(mode: ConsumerMode) -> Self {
        Self {
            mode,
            group: None,
            auto_stream_reset: AutoStreamReset::Latest,
        }
    }

    fn mode(&self) -> FileResult<&ConsumerMode> {
        Ok(&self.mode)
    }

    fn consumer_group(&self) -> FileResult<&ConsumerGroup> {
        self.group.as_ref().ok_or(StreamErr::ConsumerGroupNotSet)
    }

    /// If multiple consumers share the same group, only one in the group will receive a message.
    /// This is load-balanced in a round-robin fashion.
    fn set_consumer_group(&mut self, group: ConsumerGroup) -> FileResult<&mut Self> {
        self.group = Some(group);
        Ok(self)
    }
}

impl FileConsumerOptions {
    /// Where to stream from the file.
    ///
    /// If unset, defaults to `Latest`.
    pub fn set_auto_stream_reset(&mut self, v: AutoStreamReset) -> &mut Self {
        self.auto_stream_reset = v;
        self
    }
    pub fn auto_stream_reset(&self) -> &AutoStreamReset {
        &self.auto_stream_reset
    }
}

impl Default for FileConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

impl ProducerOptionsTrait for FileProducerOptions {}
