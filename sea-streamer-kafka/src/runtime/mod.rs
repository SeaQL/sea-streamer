#[cfg(feature = "runtime-smol")]
mod smol_impl;

#[cfg(feature = "runtime-smol")]
pub use smol_impl::SmolRuntime as KafkaAsyncRuntime;

#[cfg(feature = "runtime-tokio")]
pub use rdkafka::util::TokioRuntime as KafkaAsyncRuntime;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
mod no_rt;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use no_rt::NoRuntime as KafkaAsyncRuntime;
