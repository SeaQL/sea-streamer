use rdkafka::util::AsyncRuntime;
use std::{future::Future, pin::Pin, time::Duration};

#[derive(Debug)]
pub struct AsyncStdRuntime;

impl AsyncRuntime for AsyncStdRuntime {
    // Sadly I don't see an easy way to remove the box here
    type Delay = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        async_std::task::spawn(task);
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        Box::pin(async_std::task::sleep(duration))
    }
}
