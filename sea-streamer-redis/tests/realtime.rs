// cargo test --test realtime --features=test,runtime-tokio -- --nocapture
// cargo test --test realtime --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    const TEST: &str = "realtime";
    env_logger::init();

    use sea_streamer_redis::{AutoStreamReset, RedisConsumer, RedisConsumerOptions, RedisStreamer};
    use sea_streamer_types::{
        export::futures::StreamExt, Buffer, Consumer, ConsumerMode, ConsumerOptions, Message,
        Producer, ShardId, StreamKey, Streamer, Timestamp,
    };

    let streamer = RedisStreamer::connect(
        std::env::var("BROKERS_URL")
            .unwrap_or_else(|_| "redis://localhost".to_owned())
            .parse()
            .unwrap(),
        Default::default(),
    )
    .await?;
    let topic = StreamKey::new(format!(
        "{}-{}",
        TEST,
        Timestamp::now_utc().unix_timestamp()
    ))?;
    let zero = ShardId::new(0);

    let producer = streamer
        .create_producer(topic.clone(), Default::default())
        .await?;

    let mut sequence = 0;
    for i in 0..5 {
        let message = format!("{i}");
        let receipt = producer.send(message)?.await?;
        assert_eq!(receipt.stream_key(), &topic);
        assert!(receipt.sequence() > &sequence);
        sequence = *receipt.sequence();
        assert_eq!(receipt.shard_id(), &zero);
    }

    let mut options = RedisConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_stream_reset(AutoStreamReset::Latest);

    let mut half = streamer
        .create_consumer(&[topic.clone()], options.clone())
        .await?;

    for i in 5..10 {
        let message = format!("{i}");
        producer.send(message)?;
    }

    producer.flush().await?;

    options.set_auto_stream_reset(AutoStreamReset::Earliest);
    let mut full = streamer.create_consumer(&[topic.clone()], options).await?;

    let seq = consume(&mut half, 5).await;
    assert_eq!(seq, [5, 6, 7, 8, 9]);
    println!("Stream latest ... ok");

    let seq = consume(&mut full, 10).await;
    assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    println!("Stream history ... ok");

    async fn consume(consumer: &mut RedisConsumer, num: usize) -> Vec<usize> {
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

    Ok(())
}
