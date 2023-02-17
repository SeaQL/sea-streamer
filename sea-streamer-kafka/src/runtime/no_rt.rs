use rdkafka::util::AsyncRuntime;
use std::{
    future::{Future, Ready},
    time::Duration,
};

#[derive(Debug)]
pub struct NoRuntime;

impl AsyncRuntime for NoRuntime {
    type Delay = Ready<()>;

    fn spawn<T>(_: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        panic!("Please enable a runtime");
    }

    fn delay_for(_: Duration) -> Self::Delay {
        panic!("Please enable a runtime");
    }
}
