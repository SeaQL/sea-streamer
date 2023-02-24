#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// `sea-streamer-socket` Enum for identifying the underlying backend.
pub enum Backend {
    Kafka,
    Stdio,
}

/// `sea-streamer-socket` methods shared by `Sea*` types.
pub trait SeaStreamerBackend {
    /// Identifies the underlying backend
    fn backend(&self) -> Backend;
}
