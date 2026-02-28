use futures::future::{Either, Future, select};
use futures::pin_mut;
use std::time::Duration;

#[derive(Debug)]
pub struct TimeoutError;

pub async fn timeout<F, T>(dur: Duration, f: F) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    let timer = async_io::Timer::after(dur);
    pin_mut!(f);
    pin_mut!(timer);
    match select(f, timer).await {
        Either::Left((result, _)) => Ok(result),
        Either::Right((_, _)) => Err(TimeoutError),
    }
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TimeoutError")
    }
}

impl std::error::Error for TimeoutError {}
