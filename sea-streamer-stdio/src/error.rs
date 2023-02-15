use sea_streamer_types::StreamResult;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StdioErr {
    #[error("Flume RecvError: {0}")]
    RecvError(flume::RecvError),
    #[error("IO Error: {0}")]
    IoError(std::io::Error),
    #[error("StdioStreamer has been disconnected")]
    Disconnected,
}

pub type StdioResult<T> = StreamResult<T, StdioErr>;
