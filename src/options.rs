use crate::Result;
use std::time::Duration;

pub trait ConnectOptions: Clone + Send {
    fn timeout(&self) -> Result<Duration>;
    fn set_timeout(&mut self, d: Duration) -> Result<()>;
}
