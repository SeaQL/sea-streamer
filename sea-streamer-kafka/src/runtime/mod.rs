#[cfg(feature = "runtime-smol")]
mod smol_impl;

/// The async runtime used by rdkafka internally.
///
/// - With `runtime-tokio`: resolves to rdkafka's `TokioRuntime`.
/// - With `runtime-smol`: resolves to a custom `SmolRuntime` adapter.
/// - Without either: resolves to a stub `NoRuntime` that panics on use.
#[cfg(feature = "runtime-smol")]
pub use smol_impl::SmolRuntime as KafkaAsyncRuntime;

/// The async runtime used by rdkafka internally.
///
/// - With `runtime-tokio`: resolves to rdkafka's `TokioRuntime`.
/// - With `runtime-smol`: resolves to a custom `SmolRuntime` adapter.
/// - Without either: resolves to a stub `NoRuntime` that panics on use.
#[cfg(feature = "runtime-tokio")]
pub use rdkafka::util::TokioRuntime as KafkaAsyncRuntime;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
mod no_rt;

/// The async runtime used by rdkafka internally.
///
/// - With `runtime-tokio`: resolves to rdkafka's `TokioRuntime`.
/// - With `runtime-smol`: resolves to a custom `SmolRuntime` adapter.
/// - Without either: resolves to a stub `NoRuntime` that panics on use.
#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use no_rt::NoRuntime as KafkaAsyncRuntime;
