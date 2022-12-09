use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use std::time::Duration;

pub async fn create_producer(brokers: &str) -> FutureProducer {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", brokers);
    client_config.set("message.max.bytes", "1000000000"); // ~1Gb - this is the max that rdkafka allows

    client_config.create().unwrap()
}

// cargo run --bin producer --features=with-tokio
#[tokio::main]
async fn main() {
    let broker = std::env::var("KAFKA_BROKERS").unwrap();
    let producer = create_producer(&broker).await;
    let topic = "hello";

    println!("Connected to {}", broker);

    for i in 0..10 {
        let message = format!("{{\"hello\": {}}}", i);
        producer
            .send(
                FutureRecord::<str, String>::to(topic).payload(&message),
                Duration::from_secs(0),
            )
            .await
            .unwrap();
        println!("{}", message);
    }
}
