#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// `sea-streamer-socket` Enum for identifying the underlying backend.
pub enum Backend {
    Kafka,
    Stdio,
}

/// `sea-streamer-socket` methods shared by `Sea*` types.
pub trait SeaStreamerBackend {
    type Kafka;
    type Stdio;

    /// Identifies the underlying backend
    fn backend(&self) -> Backend;

    /// Get the concrete type for the Kafka backend. None if it's another Backend
    fn get_kafka(&mut self) -> Option<&mut Self::Kafka>;

    /// Get the concrete type for the Stdio backend. None if it's another Backend
    fn get_stdio(&mut self) -> Option<&mut Self::Stdio>;
}
