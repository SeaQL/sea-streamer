use crate::error::Result;

pub struct ClusterUri {
    pub nodes: Vec<Url>,
}

pub trait Cluster {
    type Producer: Producer;
    type Consumer: Consumer;
    type Options: ConnectOptions;

    fn connect(cluster: ClusterUri, options: Options) -> Result<Self>;

    fn create_generic_producer() -> Result<Self::Producer>;

    fn create_producer(stream: StreamKey) -> Result<Self::Producer> {
        let mut producer = Self::create_generic_producer()?;
        producer.anchor(stream);
        Ok(producer)
    }

    fn create_consumer(stream: StreamKey) -> Result<Self::Consumer>;
}