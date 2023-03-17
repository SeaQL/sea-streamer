use futures::future::{Future, FutureExt};

pub struct TaskHandle<T>(async_std::task::JoinHandle<T>);

#[derive(Debug)]
pub struct JoinError;

pub fn spawn_task<F, T>(future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    TaskHandle(async_std::task::spawn(future))
}

pub fn spawn_blocking<F, T>(future: F) -> TaskHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    TaskHandle(async_std::task::spawn_blocking(future))
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinError")
    }
}

impl std::error::Error for JoinError {}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.0.poll_unpin(cx) {
            std::task::Poll::Ready(res) => std::task::Poll::Ready(Ok(res)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
