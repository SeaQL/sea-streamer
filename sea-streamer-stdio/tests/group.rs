// cargo test --test group --features=test -- --nocapture
#[cfg(feature = "test")]
#[tokio::test]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_stdio::{
        StdioConnectOptions, StdioConsumer, StdioConsumerOptions, StdioStreamer,
    };
    use sea_streamer_types::{
        export::futures::StreamExt, Buffer, Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions,
        Message, Producer, StreamKey, Streamer, StreamerUri,
    };

    env_logger::init();

    let stream = StreamKey::new("hello")?;

    let mut options = StdioConnectOptions::default();
    options.set_loopback(true);
    let streamer = StdioStreamer::connect(StreamerUri::zero(), options).await?;

    let producer = streamer
        .create_producer(stream.clone(), Default::default())
        .await?;

    let mut consumer_opt = StdioConsumerOptions::new(ConsumerMode::LoadBalanced);
    consumer_opt.set_consumer_group(ConsumerGroup::new("abc".to_owned()))?;
    let mut first = streamer
        .create_consumer(&[stream.clone()], consumer_opt.clone())
        .await?;

    for i in 0..4 {
        let mess = format!("{}", i);
        producer.send(mess)?;
    }

    let seq = consume(&mut first, 4).await;
    assert_eq!(seq, [0, 1, 2, 3]);

    let mut second = streamer
        .create_consumer(&[stream.clone()], consumer_opt)
        .await?;

    for i in 0..10 {
        let mess = format!("{}", i);
        producer.send(mess)?;
    }

    let seq = consume(&mut first, 5).await;
    assert_eq!(seq, [0, 2, 4, 6, 8]);

    let seq = consume(&mut second, 5).await;
    assert_eq!(seq, [1, 3, 5, 7, 9]);

    streamer.disconnect().await?;

    async fn consume(consumer: &mut StdioConsumer, num: usize) -> Vec<usize> {
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
