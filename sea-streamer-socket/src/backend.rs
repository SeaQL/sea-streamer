#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Backend {
    Kafka,
    Stdio,
}

pub trait SeaStreamerBackend {
    fn backend(&self) -> Backend;
}
