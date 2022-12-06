use std::{collections::HashMap, io::Write, sync::Mutex};

use sea_streamer::{
    Producer as ProducerTrait, Sendable, SequenceNo, StreamErr, StreamKey, StreamResult, Timestamp,
};

use crate::parser::TIME_FORMAT;

lazy_static::lazy_static! {
    static ref PRODUCERS: Mutex<Producers> = Mutex::new(Default::default());
}

#[derive(Debug, Default)]
struct Producers {
    sequences: HashMap<StreamKey, SequenceNo>,
}

#[derive(Debug, Clone)]
pub struct StdioProducer {
    stream: Option<StreamKey>,
}

impl ProducerTrait for StdioProducer {
    fn send_to<S: Sendable>(&self, stream: &StreamKey, payload: S) -> StreamResult<()> {
        let seq = {
            let mut producers = PRODUCERS.lock().expect("Failed to lock Producers");
            if let Some(val) = producers.sequences.get_mut(stream) {
                let seq = *val;
                *val += 1;
                seq
            } else {
                producers.sequences.insert(stream.to_owned(), 1);
                0
            }
        };
        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();
        writeln!(
            stdout,
            "[{} | {} | {}] {}",
            Timestamp::now_utc()
                .format(TIME_FORMAT)
                .expect("Timestamp format error"),
            stream,
            seq,
            payload.as_str().map_err(StreamErr::Utf8Error)?,
        )
        .map_err(|e| StreamErr::IO(Box::new(e)))?;
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
