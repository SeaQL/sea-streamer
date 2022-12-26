use sea_streamer::StreamErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StdioStreamErr {
    #[error("Flume RecvError: {0}")]
    RecvError(flume::RecvError),
}

pub trait GetStdioErr {
    fn get(&self) -> Option<&StdioStreamErr>;
}

impl GetStdioErr for StreamErr {
    fn get(&self) -> Option<&StdioStreamErr> {
        self.reveal()
    }
}
