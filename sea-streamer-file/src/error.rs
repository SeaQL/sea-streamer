use crate::{format::FormatErr, ConfigErr};
use sea_streamer_types::{StreamErr, StreamResult};
use std::str::Utf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FileErr {
    #[error("ConfigErr: {0}")]
    ConfigErr(#[source] ConfigErr),
    #[error("Utf8Error: {0}")]
    Utf8Error(#[source] Utf8Error),
    #[error("Flume RecvError: {0}")]
    RecvError(flume::RecvError),
    #[error("IO Error: {0}")]
    IoError(#[source] std::io::Error),
    #[error("Duplicate IoError")]
    DuplicateIoError,
    #[error("Watch Error: {0}")]
    WatchError(String),
    #[error("FormatErr: {0}")]
    FormatErr(#[source] FormatErr),
    #[error("File Removed")]
    FileRemoved,
    #[error("File Limit Exceeded")]
    FileLimitExceeded,
    #[error("Task Dead ({0})")]
    TaskDead(&'static str),
    #[error("Not Enough Bytes: the file might be truncated.")]
    NotEnoughBytes,
    #[error("Stream Ended: the stream might have encountered an error or an EOS message.")]
    StreamEnded,
}

pub type FileResult<T> = StreamResult<T, FileErr>;

impl FileErr {
    /// Take ownership of this Err, leaving a clone in place.
    pub fn take(&mut self) -> Self {
        let mut copy = match self {
            FileErr::ConfigErr(e) => FileErr::ConfigErr(*e),
            FileErr::Utf8Error(e) => FileErr::Utf8Error(*e),
            FileErr::RecvError(e) => FileErr::RecvError(*e),
            FileErr::IoError(_) => FileErr::DuplicateIoError,
            FileErr::DuplicateIoError => FileErr::DuplicateIoError,
            FileErr::WatchError(e) => FileErr::WatchError(e.clone()),
            FileErr::FormatErr(e) => FileErr::FormatErr(*e),
            FileErr::FileRemoved => FileErr::FileRemoved,
            FileErr::FileLimitExceeded => FileErr::FileLimitExceeded,
            FileErr::TaskDead(e) => FileErr::TaskDead(e),
            FileErr::NotEnoughBytes => FileErr::NotEnoughBytes,
            FileErr::StreamEnded => FileErr::StreamEnded,
        };
        std::mem::swap(self, &mut copy);
        copy
    }
}

impl From<FileErr> for StreamErr<FileErr> {
    fn from(err: FileErr) -> Self {
        StreamErr::Backend(err)
    }
}
