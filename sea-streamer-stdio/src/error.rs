use sea_streamer::StreamErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StdioStreamErr {
    #[error("Flume RecvError: {0}")]
    RecvError(flume::RecvError),
}

pub trait GetStreamErr {
    fn get(&self) -> Option<&StdioStreamErr>;
}

impl GetStreamErr for StreamErr {
    fn get(&self) -> Option<&StdioStreamErr> {
        self.reveal()
    }
}
