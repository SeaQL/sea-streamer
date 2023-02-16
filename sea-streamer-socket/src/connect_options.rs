use sea_streamer_kafka::KafkaConnectOptions;
use sea_streamer_stdio::StdioConnectOptions;
use sea_streamer_types::ConnectOptions;

pub type SeaConnectOptions = KafkaConnectOptions;

pub trait SeaConnectOptionsTrait {
    fn into_stdio_connect_options(self) -> StdioConnectOptions;
    fn into_kafka_connect_options(self) -> KafkaConnectOptions;
}

impl SeaConnectOptionsTrait for SeaConnectOptions {
    fn into_stdio_connect_options(self) -> StdioConnectOptions {
        let mut options = StdioConnectOptions::default();
        if let Ok(v) = self.timeout() {
            options.set_timeout(v).expect("Never fails");
        }
        options
    }

    fn into_kafka_connect_options(self) -> KafkaConnectOptions {
        self
    }
}
