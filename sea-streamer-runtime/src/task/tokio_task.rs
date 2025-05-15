use futures::future::Future;

pub use tokio::task::{JoinError, JoinHandle as TaskHandle, spawn_blocking};

pub fn spawn_task<F, T>(future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn(future)
}
