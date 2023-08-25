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
    #[error("IO Error: {0}")]
    IoError(#[source] std::io::Error),
    #[error("Duplicate IoError")]
    DuplicateIoError,
    #[error("Watch Error: {0}")]
    WatchError(String),
    #[error("FormatErr: {0}")]
    FormatErr(#[source] FormatErr),
    #[error("SeekErr: {0}")]
    SeekErr(#[source] SeekErr),
    #[error("File Removed")]
    FileRemoved,
    #[error("File Limit Exceeded")]
    FileLimitExceeded,
    #[error("Task Dead ({0})")]
    TaskDead(&'static str),
    #[error("Not Enough Bytes: the file might be truncated.")]
    NotEnoughBytes,
    #[error("Stream Ended: the file might have been removed or an EOS message.")]
    StreamEnded,
    #[error("Producer Ended: the file might have been removed or was ended intentionally.")]
    ProducerEnded,
}

#[derive(Error, Debug, Clone, Copy)]
pub enum SeekErr {
    #[error("Out Of Bound: what you are seeking is probably not in this file")]
    OutOfBound,
    #[error("Exhausted: there must be an algorithmic error")]
    Exhausted,
}

pub type FileResult<T> = StreamResult<T, FileErr>;

impl FileErr {
    /// Take ownership of this Err, leaving a clone in place.
    pub fn take(&mut self) -> Self {
        let mut copy = match self {
            FileErr::ConfigErr(e) => FileErr::ConfigErr(*e),
            FileErr::Utf8Error(e) => FileErr::Utf8Error(*e),
            FileErr::IoError(_) => FileErr::DuplicateIoError,
            FileErr::DuplicateIoError => FileErr::DuplicateIoError,
            FileErr::WatchError(e) => FileErr::WatchError(e.clone()),
            FileErr::FormatErr(e) => FileErr::FormatErr(*e),
            FileErr::SeekErr(e) => FileErr::SeekErr(*e),
            FileErr::FileRemoved => FileErr::FileRemoved,
            FileErr::FileLimitExceeded => FileErr::FileLimitExceeded,
            FileErr::TaskDead(e) => FileErr::TaskDead(e),
            FileErr::NotEnoughBytes => FileErr::NotEnoughBytes,
            FileErr::StreamEnded => FileErr::StreamEnded,
            FileErr::ProducerEnded => FileErr::ProducerEnded,
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
