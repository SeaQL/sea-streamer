use sea_streamer::{
    Producer as ProducerTrait, Sendable, StreamErr, StreamKey, StreamResult, Timestamp,
};

#[derive(Debug, Clone)]
pub struct Producer {
    stream: Option<StreamKey>,
}

impl ProducerTrait for Producer {
    fn send_to<S: Sendable>(&self, stream: &StreamKey, payload: S) -> StreamResult<()> {
        println!(
            "[{} | {}] {}",
            Timestamp::now_utc(),
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
