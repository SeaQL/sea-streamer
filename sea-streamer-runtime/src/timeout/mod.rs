#[cfg(feature = "runtime-tokio")]
mod tokio_timeout;

#[cfg(feature = "runtime-tokio")]
pub use tokio_timeout::*;

#[cfg(feature = "runtime-async-std")]
mod async_std_timeout;

#[cfg(feature = "runtime-async-std")]
pub use async_std_timeout::*;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
mod no_rt_timeout;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use no_rt_timeout::*;
