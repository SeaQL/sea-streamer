use futures::future::{Future, FutureExt};

pub type TaskHandle<T> =
    futures::future::Map<async_std::task::JoinHandle<T>, fn(T) -> Result<T, JoinError>>;

#[derive(Debug)]
pub struct JoinError;

pub fn spawn_task<F, T>(future: F) -> TaskHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    async_std::task::spawn(future).map(Result::Ok)
}

pub fn spawn_blocking<F, T>(future: F) -> TaskHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    async_std::task::spawn_blocking(future).map(Result::Ok)
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinError")
    }
}

impl std::error::Error for JoinError {}
