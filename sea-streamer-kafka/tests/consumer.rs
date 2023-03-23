// cargo test --test consumer --features=test,runtime-tokio -- --nocapture
// cargo test --test consumer --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    use sea_streamer_kafka::{AutoOffsetReset, KafkaConsumer, KafkaConsumerOptions, KafkaStreamer};
    use sea_streamer_types::{
        export::futures::StreamExt, Buffer, Consumer, ConsumerMode, ConsumerOptions, Message,
        Producer, SeqPos, ShardId, StreamKey, Streamer, Timestamp,
    };

    let streamer = KafkaStreamer::connect(
        std::env::var("BROKERS_URL")
            .unwrap_or_else(|_| "localhost:9092".to_owned())
            .parse()
            .unwrap(),
        Default::default(),
    )
    .await?;
    let topic = StreamKey::new(format!("test-{}", Timestamp::now_utc().unix_timestamp()))?;
    let zero = ShardId::new(0);

    let producer = streamer
        .create_producer(topic.clone(), Default::default())
        .await?;

    for i in 0..7 {
        let message = format!("{i}");
        let receipt = producer.send(message)?.await?;
        assert_eq!(receipt.stream_key(), &topic);
        assert_eq!(receipt.sequence(), &i);
        assert_eq!(receipt.shard_id(), &zero);
    }
    let point_in_time = Timestamp::now_utc();
    // the seek function is supposedly up to millisecond precision, but lets not push too hard
    sea_streamer_runtime::sleep(std::time::Duration::from_secs(1)).await;
    for i in 7..10 {
        let message = format!("{i}");
        producer.send(message)?;
    }

    producer.end().await?;

    let mut options = KafkaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_offset_reset(AutoOffsetReset::Earliest);

    let mut consumer = streamer
        .create_consumer(&[topic.clone()], options.clone())
        .await?;

    let seq = consume(&mut consumer, 10).await;
    assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    println!("Basic stream ... ok");

    consumer.assign((topic.clone(), zero))?;
    consumer.rewind(SeqPos::Beginning).await?;
    let seq = consume(&mut consumer, 10).await;
    assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    println!("Rewind stream ... ok");

    consumer.rewind(SeqPos::At(5)).await?;
    let seq = consume(&mut consumer, 5).await;
    assert_eq!(seq, [5, 6, 7, 8, 9]);
    println!("Rewind to mid stream ... ok");

    // create a new consumer
    let mut options = KafkaConsumerOptions::new(ConsumerMode::Resumable);
    options.set_auto_offset_reset(AutoOffsetReset::Earliest);
    options.set_enable_auto_commit(false);

    std::mem::drop(consumer);
    let mut consumer = streamer
        .create_consumer(&[topic.clone()], options.clone())
        .await?;
    let seq = consume(&mut consumer, 10).await;
    // this should start again from beginning
    assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // commit up to 6
    consumer.commit(&topic, &zero, &6).await?;
    println!("Commit ... ok");

    // create a new consumer
    std::mem::drop(consumer);
    let mut consumer = streamer
        .create_consumer(&[topic.clone()], options.clone())
        .await?;
    let seq = consume(&mut consumer, 4).await;
    // this should resume from 6
    assert_eq!(seq, [6, 7, 8, 9]);
    println!("Resume ... ok");

    // now, seek to point in time
    consumer.seek(point_in_time).await?;
    let seq = consume(&mut consumer, 3).await;
    // this should continue from 7
    assert_eq!(seq, [7, 8, 9]);
    println!("Seek stream ... ok");

    async fn consume(consumer: &mut KafkaConsumer, num: usize) -> Vec<usize> {
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
