use sea_streamer_redis::{RedisConsumer, RedisResult};
use sea_streamer_runtime::timeout;
use sea_streamer_types::{Buffer, Consumer, Message};
use std::time::Duration;

#[allow(dead_code)]
pub async fn consume(consumer: &mut RedisConsumer, max: usize) -> RedisResult<Vec<usize>> {
    consume_impl(consumer, max, false).await
}

#[allow(dead_code)]
pub async fn consume_and_ack(consumer: &mut RedisConsumer, max: usize) -> RedisResult<Vec<usize>> {
    consume_impl(consumer, max, true).await
}

async fn consume_impl(
    consumer: &mut RedisConsumer,
    max: usize,
    ack: bool,
) -> RedisResult<Vec<usize>> {
    let mut numbers = Vec::new();
    for _ in 0..max {
        match timeout(Duration::from_secs(60), consumer.next()).await {
            Ok(mess) => {
                let mess = mess?;
                if ack {
                    consumer.ack(&mess)?;
                }
                numbers.push(mess.message().as_str().unwrap().parse::<usize>().unwrap());
            }
            Err(_) => panic!("Timed out when streaming up to {numbers:?}"),
        }
    }
    Ok(numbers)
}
