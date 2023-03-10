use futures::future::Future;
use std::time::Duration;
pub use tokio::time::error::Elapsed as TimeoutError;

pub async fn timeout<F, T>(dur: Duration, f: F) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    tokio::time::timeout(dur, f).await
}
