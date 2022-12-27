use crate::{KafkaErr, KafkaResult};
use sea_streamer::{Producer, ProducerOptions, Sendable, StreamKey};

#[derive(Debug, Clone)]
pub struct KafkaProducer {
    stream: Option<StreamKey>,
}

#[derive(Debug, Default, Clone)]
pub struct KafkaProducerOptions {}

impl Producer for KafkaProducer {
    type Error = KafkaErr;

    fn send_to<S: Sendable>(&self, stream: &StreamKey, payload: S) -> KafkaResult<()> {
        unimplemented!()
    }

    fn anchor(&mut self, stream: StreamKey) -> KafkaResult<()> {
        unimplemented!()
    }

    fn anchored(&self) -> KafkaResult<&StreamKey> {
        unimplemented!()
    }
}

impl ProducerOptions for KafkaProducerOptions {}
