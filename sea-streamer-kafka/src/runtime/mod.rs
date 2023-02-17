#[cfg(feature = "runtime-async-std")]
mod async_std_impl;

#[cfg(feature = "runtime-async-std")]
pub use async_std_impl::AsyncStdRuntime as KafkaAsyncRuntime;

#[cfg(feature = "runtime-tokio")]
pub use rdkafka::util::TokioRuntime as KafkaAsyncRuntime;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
mod no_rt;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use no_rt::NoRuntime as KafkaAsyncRuntime;
