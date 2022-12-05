use crate::{Result, Sendable, StreamKey};

pub trait ProducerOptions: Default + Clone + Send {}

pub trait Producer: Clone + Send + Sync {
    /// Send a message to a particular stream
    fn send_to<S: Sendable>(&self, stream: &StreamKey, payload: S) -> Result<()>;
    /// Send a message to the already anchored stream. This function is non-blocking.
    /// If the producer was not anchored, this will return [`NotAnchored`] error.
    fn send<S: Sendable>(&self, payload: S) -> Result<()> {
        self.send_to(self.anchored()?, payload)
    }
    /// Lock this producer to a particular stream. This function can only be called once.
    /// Subsequent calls should return [`AlreadyAnchored`] error.
    fn anchor(&mut self, stream: StreamKey) -> Result<()>;
    /// If the producer is already anchored, return a reference to the StreamKey
    /// If the producer was not anchored, this will return [`NotAnchored`] error.
    fn anchored(&self) -> Result<&StreamKey>;
}
