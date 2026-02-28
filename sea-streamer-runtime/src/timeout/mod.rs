#[cfg(feature = "runtime-tokio")]
mod tokio_timeout;

#[cfg(feature = "runtime-tokio")]
pub use tokio_timeout::*;

#[cfg(feature = "runtime-smol")]
mod smol_timeout;

#[cfg(feature = "runtime-smol")]
pub use smol_timeout::*;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
mod no_rt_timeout;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use no_rt_timeout::*;
