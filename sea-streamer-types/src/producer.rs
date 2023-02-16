use futures::Future;

use crate::{MessageHeader, Sendable, StreamKey, StreamResult};

pub trait ProducerOptions: Default + Clone + Send {}

pub type Receipt = MessageHeader;

pub trait Producer: Clone + Send + Sync {
    type Error: std::error::Error;
    type SendFuture: Future<Output = StreamResult<Receipt, Self::Error>>;

    /// Send a message to a particular stream. This function is non-blocking.
    /// You don't have to await the future if you are not interested in the Receipt.
    fn send_to<S: Sendable>(
        &self,
        stream: &StreamKey,
        payload: S,
    ) -> StreamResult<Self::SendFuture, Self::Error>;

    /// Send a message to the already anchored stream.
    /// If the producer is not anchored, this will return [`StreamErr::NotAnchored`] error.
    fn send<S: Sendable>(&self, payload: S) -> StreamResult<Self::SendFuture, Self::Error> {
        self.send_to(self.anchored()?, payload)
    }

    /// Lock this producer to a particular stream. This function can only be called once.
    /// Subsequent calls should return [`AlreadyAnchored`] error.
    fn anchor(&mut self, stream: StreamKey) -> StreamResult<(), Self::Error>;

    /// If the producer is already anchored, return a reference to the StreamKey
    /// If the producer is not anchored, this will return [`StreamErr::NotAnchored`] error.
    fn anchored(&self) -> StreamResult<&StreamKey, Self::Error>;
}
