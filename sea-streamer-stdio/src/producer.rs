use sea_streamer::{
    Producer as ProducerTrait, Sendable, StreamErr, StreamKey, StreamResult, Timestamp,
};

use crate::parser::TIME_FORMAT;

#[derive(Debug, Clone)]
pub struct StdioProducer {
    stream: Option<StreamKey>,
}

impl ProducerTrait for StdioProducer {
    fn send_to<S: Sendable>(&self, stream: &StreamKey, payload: S) -> StreamResult<()> {
        println!(
            "[{} | {}] {}",
            Timestamp::now_utc()
                .format(TIME_FORMAT)
                .expect("Timestamp format error"),
            stream,
            payload.as_str().map_err(StreamErr::Utf8Error)?
        );
        Ok(())
    }

    fn anchor(&mut self, stream: StreamKey) -> StreamResult<()> {
        if self.stream.is_none() {
            self.stream = Some(stream);
            Ok(())
        } else {
            Err(StreamErr::AlreadyAnchored)
        }
    }

    fn anchored(&self) -> StreamResult<&StreamKey> {
        if let Some(stream) = &self.stream {
            Ok(stream)
        } else {
            Err(StreamErr::NotAnchored)
        }
    }
}

impl StdioProducer {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { stream: None }
    }
}
