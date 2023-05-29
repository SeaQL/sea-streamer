#[cfg(feature = "runtime-tokio")]
pub use tokio::sync::Mutex as AsyncMutex;

#[cfg(feature = "runtime-async-std")]
pub use async_std::sync::Mutex as AsyncMutex;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use std::sync::Mutex as AsyncMutex;