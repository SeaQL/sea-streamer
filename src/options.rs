use crate::StreamResult;
use std::time::Duration;

pub trait ConnectOptions: Default + Clone + Send {
    type Error: std::error::Error;

    fn timeout(&self) -> StreamResult<Duration, Self::Error>;
    fn set_timeout(&mut self, d: Duration) -> StreamResult<&mut Self, Self::Error>;
}
