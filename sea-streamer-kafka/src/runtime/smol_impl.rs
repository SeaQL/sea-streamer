use rdkafka::util::AsyncRuntime;
use std::{future::Future, pin::Pin, time::Duration};

#[derive(Debug)]
pub struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        smol::spawn(task).detach();
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        Box::pin(async move {
            smol::Timer::after(duration).await;
        })
    }
}
