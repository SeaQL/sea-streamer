use futures::future::{Future, FutureExt};

/// Wraps `smol::Task` with detach-on-drop semantics to match tokio's behavior.
/// In smol, dropping a `Task` cancels it; we detach instead so fire-and-forget
/// spawns continue running in the background.
pub struct TaskHandle<T>(Option<smol::Task<T>>);

#[derive(Debug)]
pub struct JoinError;

pub fn spawn_task<F, T>(future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    TaskHandle(Some(smol::spawn(future)))
}

pub fn spawn_blocking<F, T>(f: F) -> TaskHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    TaskHandle(Some(smol::unblock(f)))
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinError")
    }
}

impl std::error::Error for JoinError {}

impl<T> Drop for TaskHandle<T> {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.detach();
        }
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.0.as_mut() {
            Some(task) => match task.poll_unpin(cx) {
                std::task::Poll::Ready(res) => std::task::Poll::Ready(Ok(res)),
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => std::task::Poll::Ready(Err(JoinError)),
        }
    }
}
