use sea_streamer::StreamResult;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StdioErr {
    #[error("Flume RecvError: {0}")]
    RecvError(flume::RecvError),
    #[error("IO Error: {0}")]
    IoError(std::io::Error),
}

pub type StdioResult<T> = StreamResult<T, StdioErr>;
