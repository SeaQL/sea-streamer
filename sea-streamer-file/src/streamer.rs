use std::time::Duration;
use thiserror::Error;

use crate::{
    consumer::new_consumer, end_producer, format::Header, new_producer, AsyncFile, FileConsumer,
    FileErr, FileId, FileProducer, FileResult, DEFAULT_BEACON_INTERVAL, DEFAULT_FILE_SIZE_LIMIT,
    DEFAULT_PREFETCH_MESSAGE,
};
use sea_streamer_types::{
    export::async_trait, ConnectOptions as ConnectOptionsTrait, ConsumerGroup, ConsumerMode,
    ConsumerOptions as ConsumerOptionsTrait, ProducerOptions as ProducerOptionsTrait, StreamErr,
    StreamKey, StreamUrlErr, Streamer as StreamerTrait, StreamerUri,
};

#[derive(Debug, Clone)]
pub struct FileStreamer {
    file_id: FileId,
    options: FileConnectOptions,
}

#[derive(Debug, Clone)]
pub struct FileConnectOptions {
    create_file: CreateFileOption,
    end_with_eos: bool,
    beacon_interval: u32,
    file_size_limit: u64,
    prefetch_message: usize,
}

#[derive(Debug, Clone)]
enum CreateFileOption {
    /// File must already exists
    Never,
    CreateIfNotExists,
    /// Fail if the file already exists
    Always,
}

#[derive(Debug, Clone)]
pub struct FileConsumerOptions {
    mode: ConsumerMode,
    group: Option<ConsumerGroup>,
    auto_stream_reset: AutoStreamReset,
    live_streaming: bool,
}

#[derive(Debug, Default, Clone)]
pub struct FileProducerOptions {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Where to start streaming from.
pub enum AutoStreamReset {
    Earliest,
    Latest,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum StreamMode {
    /// Streaming from a file at the end
    Live,
    /// Replaying a dead file
    Replay,
    /// Replaying a live file, might catch up to live
    LiveReplay,
}

#[derive(Error, Debug, Clone, Copy)]
pub enum ConfigErr {
    #[error("Cannot stream from a non-live file at the end")]
    LatestButNotLive,
    #[error("Consumers in the same ConsumerGroup must use the same ConsumerMode")]
    SameGroupSameMode,
    #[error("Please choose a 'better aligned' beacon interval")]
    InvalidBeaconInterval,
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
    /// If not, depending on the options, either create it, or error.
    async fn connect(uri: StreamerUri, options: Self::ConnectOptions) -> FileResult<Self> {
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
        let file_id = FileId::new(path);
        match options.create_file {
            CreateFileOption::Never => AsyncFile::new_r(file_id.clone()).await,
            CreateFileOption::CreateIfNotExists => AsyncFile::new_rw(file_id.clone()).await,
            CreateFileOption::Always => AsyncFile::new_w(file_id.clone()).await,
        }?;
        Ok(Self { file_id, options })
    }

    /// End the producers before disconnecting.
    async fn disconnect(self) -> FileResult<()> {
        match end_producer(self.file_id).recv_async().await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(StreamErr::Backend(e)),
            Err(_) => Err(StreamErr::Backend(FileErr::ProducerEnded)),
        }
    }

    async fn create_generic_producer(
        &self,
        options: Self::ProducerOptions,
    ) -> FileResult<Self::Producer> {
        new_producer(self.file_id.clone(), &self.options, &options)
            .await
            .map_err(StreamErr::Backend)
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
        let stream_mode = match (options.auto_stream_reset, options.live_streaming) {
            (AutoStreamReset::Latest, true) => StreamMode::Live,
            (AutoStreamReset::Earliest, true) => {
                if options.group.is_none() {
                    let file = AsyncFile::new_r(self.file_id.clone()).await?;
                    if file.size() <= Header::size() as u64 {
                        // special case when the file has no data
                        StreamMode::Live
                    } else {
                        StreamMode::LiveReplay
                    }
                } else {
                    StreamMode::LiveReplay
                }
            }
            (AutoStreamReset::Earliest, false) => StreamMode::Replay,
            (AutoStreamReset::Latest, false) => {
                return Err(StreamErr::Backend(FileErr::ConfigErr(
                    ConfigErr::LatestButNotLive,
                )))
            }
        };

        let consumer = new_consumer(
            self.file_id.clone(),
            stream_mode,
            options.group,
            streams.to_vec(),
            self.options.prefetch_message,
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

impl Default for FileConnectOptions {
    fn default() -> Self {
        Self {
            create_file: CreateFileOption::Never,
            end_with_eos: false,
            beacon_interval: DEFAULT_BEACON_INTERVAL,
            file_size_limit: DEFAULT_FILE_SIZE_LIMIT,
            prefetch_message: DEFAULT_PREFETCH_MESSAGE,
        }
    }
}

impl FileConnectOptions {
    pub fn create_if_not_exists(&self) -> bool {
        matches!(self.create_file, CreateFileOption::CreateIfNotExists)
    }
    /// Default is `false`.
    pub fn set_create_if_not_exists(&mut self, v: bool) -> &mut Self {
        if v {
            self.create_file = CreateFileOption::CreateIfNotExists;
        } else {
            self.create_file = CreateFileOption::Never;
        }
        self
    }

    pub fn create_only(&self) -> bool {
        matches!(self.create_file, CreateFileOption::Always)
    }
    /// Always create the file. Fail if already exists. Default is `false`.
    pub fn set_create_only(&mut self, v: bool) -> &mut Self {
        if v {
            self.create_file = CreateFileOption::Always;
        } else {
            self.create_file = CreateFileOption::Never;
        }
        self
    }

    pub fn end_with_eos(&self) -> bool {
        self.end_with_eos
    }
    /// If true, when the producer ends, a End-of-Stream message will be written.
    /// This signals Consumers to end their streams.
    ///
    /// Default is `false`.
    pub fn set_end_with_eos(&mut self, v: bool) -> &mut Self {
        self.end_with_eos = v;
        self
    }

    pub fn beacon_interval(&self) -> u32 {
        self.beacon_interval
    }
    /// Beacon interval. Should be multiples of 1024 (1KB).
    ///
    /// Default is [`crate::DEFAULT_BEACON_INTERVAL`].
    pub fn set_beacon_interval(&mut self, v: u32) -> Result<&mut Self, FileErr> {
        let valid = v > 0 && v % 1024 == 0;
        if !valid {
            return Err(FileErr::ConfigErr(ConfigErr::InvalidBeaconInterval));
        }
        self.beacon_interval = v;
        Ok(self)
    }

    pub fn file_size_limit(&self) -> u64 {
        self.file_size_limit
    }
    /// Default is [`crate::DEFAULT_FILE_SIZE_LIMIT`].
    pub fn set_file_size_limit(&mut self, v: u64) -> &mut Self {
        self.file_size_limit = v;
        self
    }

    pub fn prefetch_message(&self) -> usize {
        self.prefetch_message
    }

    /// Number of messages to prefetch. A larger number would lead to higher memory usage.
    /// Choose the number by considering the typical size of messages.
    ///
    /// Default is [`crate::DEFAULT_PREFETCH_MESSAGE`].
    pub fn set_prefetch_message(&mut self, v: usize) -> &mut Self {
        self.prefetch_message = v;
        self
    }
}

impl ConsumerOptionsTrait for FileConsumerOptions {
    type Error = FileErr;

    fn new(mode: ConsumerMode) -> Self {
        Self {
            mode,
            group: None,
            auto_stream_reset: AutoStreamReset::Latest,
            live_streaming: true,
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

    /// If true, follow the file like `tail -f` and read new messages as there is more data.
    ///
    /// If unset, defaults to `true`.
    pub fn set_live_streaming(&mut self, v: bool) -> &mut Self {
        self.live_streaming = v;
        self
    }
    pub fn live_streaming(&self) -> &bool {
        &self.live_streaming
    }
}

impl Default for FileConsumerOptions {
    fn default() -> Self {
        Self::new(ConsumerMode::RealTime)
    }
}

impl ProducerOptionsTrait for FileProducerOptions {}
