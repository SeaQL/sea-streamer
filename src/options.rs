use crate::Result;
use std::time::Duration;

pub trait ConnectOptions: Default + Clone + Send {
    fn timeout(&self) -> Result<Duration>;
    fn set_timeout(&mut self, d: Duration) -> Result<&mut Self>;
}
