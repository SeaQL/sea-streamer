use sea_streamer_redis::RedisConsumer;
use sea_streamer_runtime::timeout;
use sea_streamer_types::{Buffer, Consumer, Message};
use std::time::Duration;

pub async fn consume(consumer: &mut RedisConsumer, max: usize) -> Vec<usize> {
    let mut numbers = Vec::new();
    for _ in 0..max {
        match timeout(Duration::from_secs(60), consumer.next()).await {
            Ok(mess) => numbers.push(
                mess.unwrap()
                    .message()
                    .as_str()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap(),
            ),
            Err(_) => panic!("Timed out when streaming up to {numbers:?}"),
        }
    }
    numbers
}
