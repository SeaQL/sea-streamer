#[cfg(feature = "runtime-tokio")]
mod tokio_task;

#[cfg(feature = "runtime-tokio")]
pub use tokio_task::*;

#[cfg(feature = "runtime-smol")]
mod smol_task;

#[cfg(feature = "runtime-smol")]
pub use smol_task::*;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
mod no_rt_task;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use no_rt_task::*;
