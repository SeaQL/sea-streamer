use futures::Future;

use crate::{Buffer, MessageHeader, StreamKey, StreamResult};

/// Common options of a Producer.
pub trait ProducerOptions: Default + Clone + Send {}

/// Delivery receipt.
pub type Receipt = MessageHeader;

/// Common interface of producers, to be implemented by all backends.
pub trait Producer: Clone + Send + Sync {
    type Error: std::error::Error;
    type SendFuture: Future<Output = StreamResult<Receipt, Self::Error>>;

    /// Send a message to a particular stream. This function is non-blocking.
    /// You don't have to await the future if you are not interested in the Receipt.
    fn send_to<S: Buffer>(
        &self,
        stream: &StreamKey,
        payload: S,
    ) -> StreamResult<Self::SendFuture, Self::Error>;

    /// Send a message to the already anchored stream. This function is non-blocking.
    /// You don't have to await the future if you are not interested in the Receipt.
    ///
    /// If the producer is not anchored, this will return `StreamErr::NotAnchored` error.
    fn send<S: Buffer>(&self, payload: S) -> StreamResult<Self::SendFuture, Self::Error> {
        self.send_to(self.anchored()?, payload)
    }

    /// Lock this producer to a particular stream. This function can only be called once.
    /// Subsequent calls should return `StreamErr::AlreadyAnchored` error.
    fn anchor(&mut self, stream: StreamKey) -> StreamResult<(), Self::Error>;

    /// If the producer is already anchored, return a reference to the StreamKey.
    /// If the producer is not anchored, this will return `StreamErr::NotAnchored` error.
    fn anchored(&self) -> StreamResult<&StreamKey, Self::Error>;
}
