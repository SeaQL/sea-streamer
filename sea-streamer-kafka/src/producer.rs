use sea_streamer::{Producer, ProducerOptions, Sendable, StreamKey, StreamResult};

#[derive(Debug, Clone)]
pub struct KafkaProducer {
    stream: Option<StreamKey>,
}

#[derive(Debug, Default, Clone)]
pub struct KafkaProducerOptions {}

impl Producer for KafkaProducer {
    fn send_to<S: Sendable>(&self, stream: &StreamKey, payload: S) -> StreamResult<()> {
        unimplemented!()
    }

    fn anchor(&mut self, stream: StreamKey) -> StreamResult<()> {
        unimplemented!()
    }

    fn anchored(&self) -> StreamResult<&StreamKey> {
        unimplemented!()
    }
}

impl ProducerOptions for KafkaProducerOptions {}
