// Integration test: needs an Iggy server whose wire protocol matches the `iggy` crate in Cargo.toml
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    use sea_streamer_iggy::{IggyConnectOptions, IggyConsumerOptions, IggyPollingStrategy};
    use sea_streamer_types::{
        export::futures::StreamExt, Buffer, Consumer, ConsumerMode, ConsumerOptions, Message,
        Producer, StreamKey, Streamer, Timestamp,
    };

    let url = std::env::var("IGGY_URL").unwrap_or_else(|_| "iggy://127.0.0.1:8090".to_owned());

    let mut connect_options = IggyConnectOptions::default();
    connect_options.set_url(&url);

    let streamer = sea_streamer_iggy::IggyStreamer::connect(url.parse()?, connect_options).await?;

    let stream_name = format!("test-stream-{}", Timestamp::now_utc().unix_timestamp());
    let topic_name = format!("test-topic-{}", Timestamp::now_utc().unix_timestamp());

    let mut producer_options = sea_streamer_iggy::IggyProducerOptions::default();
    producer_options.set_stream_name(&stream_name);
    producer_options.set_topic_name(&topic_name);
    producer_options.set_create_stream_if_not_exists(true);
    producer_options.set_create_topic_if_not_exists(true);

    let mut producer = streamer.create_generic_producer(producer_options).await?;

    let topic = StreamKey::new(&topic_name)?;
    let stream = StreamKey::new(&stream_name)?;

    for i in 0..5 {
        let message = format!("message-{i}");
        producer.send_to(Some(&stream), &topic, message)?.await?;
    }
    producer.flush().await?;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut consumer_options = IggyConsumerOptions::new(ConsumerMode::RealTime);
    consumer_options.set_stream_name(&stream_name);
    consumer_options.set_topic_name(&topic_name);
    consumer_options.set_polling_strategy(IggyPollingStrategy::First);
    consumer_options.set_polling_interval_ms(100);
    consumer_options.set_batch_size(10);

    let mut consumer = streamer
        .create_consumer(&[StreamKey::new(&stream_name)?, StreamKey::new(&topic_name)?], consumer_options)
        .await?;

    let mut received = Vec::new();
    let mut stream = consumer.stream();
    for _ in 0..5 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await?
            .expect("stream should yield a message")
            .expect("message should be Ok");
        received.push(msg.message().as_str().unwrap().to_owned());
    }
    drop(stream);

    assert_eq!(
        received,
        vec!["message-0", "message-1", "message-2", "message-3", "message-4"]
    );
    println!("Produce and consume ... ok");

    streamer.disconnect().await?;

    Ok(())
}
