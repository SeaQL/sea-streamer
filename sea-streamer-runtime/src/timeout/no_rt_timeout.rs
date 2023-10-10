use std::{future::Future, time::Duration};

#[derive(Debug)]
pub struct TimeoutError;

pub async fn timeout<F, T>(_: Duration, _f: F) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    Err(TimeoutError)
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Please enable a runtime")
    }
}

impl std::error::Error for TimeoutError {}
