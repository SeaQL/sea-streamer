#![allow(unreachable_code)]
use std::time::Duration;

#[inline]
pub async fn sleep(_s: Duration) {
    #[cfg(feature = "runtime-async-std")]
    return async_std::task::sleep(_s).await;

    #[cfg(feature = "runtime-tokio")]
    return tokio::time::sleep(_s).await;

    panic!("Please enable a runtime");
}
