use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

pub struct FutureResult<T> {
    phantom: PhantomData<T>,
}
pub use FutureResult as TaskHandle;

#[derive(Debug)]
pub struct Error;

pub fn spawn_task<F, T>(_: F) -> FutureResult<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    FutureResult {
        phantom: PhantomData,
    }
}

pub use spawn_task as spawn_blocking;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Not implemented")
    }
}

impl std::error::Error for Error {}

impl<T> Future for FutureResult<T> {
    type Output = Result<T, Error>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<<Self as Future>::Output> {
        Poll::Ready(Err(Error))
    }
}
