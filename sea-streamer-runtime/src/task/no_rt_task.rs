use futures::future::{ready, Ready};
use std::future::Future;

#[derive(Debug)]
pub struct Error;

pub fn spawn_task<F, T>(_: F) -> Ready<Result<T, Error>>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    ready(Err(Error))
}

pub fn spawn_blocking<F, T>(_: F) -> Ready<Result<T, Error>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    ready(Err(Error))
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Not implemented")
    }
}

impl std::error::Error for Error {}
