use std::{collections::HashMap, io::Write, sync::Mutex};

use sea_streamer::{
    export::futures::future::{ready, Ready},
    MessageMeta, Producer as ProducerTrait, Sendable, SequenceNo, ShardId, StreamErr, StreamKey,
    Timestamp,
};

use crate::{parser::TIME_FORMAT, StdioErr, StdioResult};

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

const ZERO: u64 = 0;

impl ProducerTrait for StdioProducer {
    type Error = StdioErr;
    type SendFuture = Ready<Result<MessageMeta, StdioErr>>;

    /// The current implementation blocks. In the future we might spawn a background thread
    /// to handle write if true non-blocking behaviour is desired.
    fn send_to<S: Sendable>(
        &self,
        stream: &StreamKey,
        payload: S,
    ) -> StdioResult<Self::SendFuture> {
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
        let now = Timestamp::now_utc();
        let receipt = MessageMeta::new(stream.to_owned(), ShardId::new(ZERO), seq, now);
        writeln!(
            stdout,
            "[{timestamp} | {stream} | {seq}] {payload}",
            timestamp = now.format(TIME_FORMAT).expect("Timestamp format error"),
            payload = payload.as_str().map_err(StreamErr::Utf8Error)?,
        )
        .map_err(|e| StreamErr::Backend(StdioErr::IoError(e)))?;
        Ok(ready(Ok(receipt)))
    }

    fn anchor(&mut self, stream: StreamKey) -> StdioResult<()> {
        if self.stream.is_none() {
            self.stream = Some(stream);
            Ok(())
        } else {
            Err(StreamErr::AlreadyAnchored)
        }
    }

    fn anchored(&self) -> StdioResult<&StreamKey> {
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
