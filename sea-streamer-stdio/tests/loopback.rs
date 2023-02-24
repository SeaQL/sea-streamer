// cargo test --test loopback --features=test -- --nocapture
#[cfg(feature = "test")]
#[tokio::test]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_stdio::{StdioConnectOptions, StdioConsumer, StdioStreamer};
    use sea_streamer_types::{
        export::futures::StreamExt, Consumer, Message, Producer, Sendable, StreamKey, Streamer,
        StreamerUri,
    };

    env_logger::init();

    let stream = StreamKey::new("hello".to_owned());
    let streamer = StdioStreamer::connect(StreamerUri::zero(), Default::default()).await?;
    let consumer = streamer
        .create_consumer(&[stream.clone()], Default::default())
        .await?;
    let producer = streamer
        .create_producer(stream.clone(), Default::default())
        .await?;

    for _ in 0..5 {
        let mess = format!("{}", -1);
        // these are not looped back
        producer.send(mess)?;
    }

    streamer.disconnect().await?;
    assert!(consumer.next().await.is_err());
    assert!(producer.send("").is_err());

    let mut options = StdioConnectOptions::default();
    options.set_loopback(true);
    let streamer = StdioStreamer::connect(StreamerUri::zero(), options).await?;
    let producer = streamer
        .create_producer(stream.clone(), Default::default())
        .await?;
    let mut consumer = streamer
        .create_consumer(&[stream.clone()], Default::default())
        .await?;

    for i in 0..5 {
        let mess = format!("{}", i);
        producer.send(mess)?;
    }

    let seq = consume(&mut consumer, 5).await;
    assert_eq!(seq, [0, 1, 2, 3, 4]);

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
