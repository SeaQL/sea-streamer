#[cfg(feature = "runtime-tokio")]
mod tokio_task;

#[cfg(feature = "runtime-tokio")]
pub use tokio_task::*;

#[cfg(feature = "runtime-async-std")]
mod async_std_task;

#[cfg(feature = "runtime-async-std")]
pub use async_std_task::*;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
mod no_rt_task;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use no_rt_task::*;
