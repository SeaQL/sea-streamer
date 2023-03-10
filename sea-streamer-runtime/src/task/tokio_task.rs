use futures::future::Future;

pub use tokio::task::{spawn_blocking, JoinError, JoinHandle as TaskHandle};

pub fn spawn_task<F, T>(future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn(future)
}
