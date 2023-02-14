use anyhow::Result;
use sea_streamer::{
    export::futures::StreamExt, Consumer, ConsumerMode, ConsumerOptions, Message, Producer,
    Sendable, ShardId, StreamKey, Streamer, StreamerUri, Timestamp,
};
use sea_streamer_kafka::{AutoOffsetReset, KafkaConsumer, KafkaConsumerOptions, KafkaStreamer};

#[tokio::test]
async fn main() -> Result<()> {
    let streamer = KafkaStreamer::connect(
        StreamerUri::one(
            std::env::var("BROKERS_URL")
                .unwrap_or_else(|_| "localhost:9092".to_owned())
                .parse()
                .unwrap(),
        ),
        Default::default(),
    )
    .await?;
    let topic = StreamKey::new(format!("basic-{}", Timestamp::now_utc().unix_timestamp()));
    let zero = ShardId::new(0);

    let producer = streamer
        .create_producer(topic.clone(), Default::default())
        .await?;

    for i in 0..10 {
        let message = format!("{i}");
        producer.send(message)?;
    }

    producer.flush(std::time::Duration::from_secs(60)).await?;

    let mut options = KafkaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_offset_reset(AutoOffsetReset::Earliest);

    let mut consumer = streamer
        .create_consumer(&[topic.clone()], options.clone())
        .await?;

    let seq = consume(&consumer, 10).await;
    assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    consumer.assign(zero)?;
    consumer.rewind(sea_streamer::SequencePos::Beginning)?;
    let seq = consume(&consumer, 10).await;
    assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    consumer.rewind(sea_streamer::SequencePos::At(5))?;
    let seq = consume(&consumer, 5).await;
    assert_eq!(seq, [5, 6, 7, 8, 9]);

    // create a new consumer
    let mut options = KafkaConsumerOptions::new(ConsumerMode::Resumable);
    options.set_auto_offset_reset(AutoOffsetReset::Earliest);
    options.set_enable_auto_commit(false);

    std::mem::drop(consumer);
    let mut consumer = streamer
        .create_consumer(&[topic.clone()], options.clone())
        .await?;
    let seq = consume(&consumer, 10).await;
    // this should start again from beginning
    assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // commit up to 6
    consumer.commit(&topic, &zero, &6).await?;

    // create a new consumer
    std::mem::drop(consumer);
    let consumer = streamer
        .create_consumer(&[topic.clone()], options.clone())
        .await?;
    let seq = consume(&consumer, 4).await;
    // this should resume from 6
    assert_eq!(seq, [6, 7, 8, 9]);

    Ok(())
}

async fn consume(consumer: &KafkaConsumer, num: usize) -> Vec<usize> {
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
