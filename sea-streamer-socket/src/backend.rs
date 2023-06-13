#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// `sea-streamer-socket` Enum for identifying the underlying backend.
pub enum Backend {
    #[cfg(feature = "backend-kafka")]
    Kafka,
    #[cfg(feature = "backend-redis")]
    Redis,
    #[cfg(feature = "backend-stdio")]
    Stdio,
    #[cfg(feature = "backend-file")]
    File,
}

/// `sea-streamer-socket` methods shared by `Sea*` types.
pub trait SeaStreamerBackend {
    #[cfg(feature = "backend-kafka")]
    type Kafka;
    #[cfg(feature = "backend-redis")]
    type Redis;
    #[cfg(feature = "backend-stdio")]
    type Stdio;
    #[cfg(feature = "backend-file")]
    type File;

    /// Identifies the underlying backend
    fn backend(&self) -> Backend;

    #[cfg(feature = "backend-kafka")]
    /// Get the concrete type for the Kafka backend. None if it's another Backend
    fn get_kafka(&mut self) -> Option<&mut Self::Kafka>;

    #[cfg(feature = "backend-redis")]
    /// Get the concrete type for the Redis backend. None if it's another Backend
    fn get_redis(&mut self) -> Option<&mut Self::Redis>;

    #[cfg(feature = "backend-stdio")]
    /// Get the concrete type for the Stdio backend. None if it's another Backend
    fn get_stdio(&mut self) -> Option<&mut Self::Stdio>;

    #[cfg(feature = "backend-file")]
    /// Get the concrete type for the File backend. None if it's another Backend
    fn get_file(&mut self) -> Option<&mut Self::File>;
}
