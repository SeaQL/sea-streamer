use sea_streamer_redis::RedisConsumer;
use sea_streamer_types::{export::futures::StreamExt, Buffer, Consumer, Message};

pub async fn consume(consumer: &mut RedisConsumer, num: usize) -> Vec<usize> {
    consumer
        .stream()
        .take(num)
        .map(|mess| {
            mess.unwrap()
                .message()
                .as_str()
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .collect::<Vec<usize>>()
        .await
}
