#[cfg(feature = "runtime-tokio")]
pub use tokio::task::{spawn as spawn_task, spawn_blocking, JoinHandle as TaskHandle};

#[cfg(feature = "runtime-async-std")]
mod async_std_task;

#[cfg(feature = "runtime-async-std")]
pub use async_std_task::*;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
mod no_rt_task;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use no_rt_task::*;
