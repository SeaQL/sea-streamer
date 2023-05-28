use crate::format::{FormatErr, HeaderErr};
use sea_streamer_types::StreamResult;
use std::str::Utf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FileErr {
    #[error("Utf8Error: {0}")]
    Utf8Error(#[source] Utf8Error),
    #[error("IO Error: {0}")]
    IoError(#[source] std::io::Error),
    #[error("Watch Error: {0}")]
    WatchError(#[source] notify::Error),
    // #[error("Flume RecvError: {0}")]
    // RecvError(#[source] flume::RecvError),
    #[error("HeaderErr: {0}")]
    HeaderErr(#[source] HeaderErr),
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
}

pub type FileResult<T> = StreamResult<T, FileErr>;
