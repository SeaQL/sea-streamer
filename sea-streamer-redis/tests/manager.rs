// cargo test --test manager --features=test,runtime-tokio -- --nocapture
// cargo test --test manager --features=test,runtime-tokio,nanosecond-timestamp -- --nocapture
// cargo test --test manager --no-default-features --features=test,runtime-smol -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-smol", smol_potat::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        IdRange, RedisConnectOptions, RedisMessageId, RedisStreamer, TrimMode,
    };
    use sea_streamer_types::{Buffer, Message, Producer, StreamKey, Streamer, Timestamp};

    const TEST: &str = "manager";
    env_logger::init();

    #[allow(unused_mut)]
    let mut options = RedisConnectOptions::default();
    #[cfg(feature = "nanosecond-timestamp")]
    options.set_timestamp_format(sea_streamer_redis::TimestampFormat::UnixTimestampNanos);
    let streamer = RedisStreamer::connect(
        std::env::var("BROKERS_URL")
            .unwrap_or_else(|_| "redis://localhost".to_owned())
            .parse()
            .unwrap(),
        options,
    )
    .await?;
    println!("Connect Streamer ... ok");

    let now = Timestamp::now_utc();
    let stream = StreamKey::new(format!(
        "{}-{}",
        TEST,
        now.unix_timestamp_nanos() / 1_000_000
    ))?;
    let missing = StreamKey::new(format!(
        "{}-{}-missing",
        TEST,
        now.unix_timestamp_nanos() / 1_000_000
    ))?;

    let producer = streamer.create_generic_producer(Default::default()).await?;
    let mut timestamps = Vec::new();
    for i in 0..10 {
        // distinct IDs even under millisecond auto IDs
        sea_streamer_runtime::sleep(std::time::Duration::from_millis(2)).await;
        let receipt = producer.send_to(&stream, format!("{i}"))?.await?;
        timestamps.push(*receipt.timestamp());
    }
    producer.end().await?;
    println!("Produce 10 messages ... ok");

    let mut manager = streamer.create_manager().await?;

    assert_eq!(manager.len(&missing).await?, 0);
    assert_eq!(manager.len(&stream).await?, 10);
    println!("XLEN ... ok");

    // Approximate trimming may remove nothing on a tiny stream; it must still succeed.
    manager.trim_max_len(&stream, 8, TrimMode::Approx).await?;
    assert!(manager.len(&stream).await? >= 8);

    let before = manager.len(&stream).await?;
    assert_eq!(
        manager.trim_max_len(&stream, 8, TrimMode::Exact).await?,
        before - 8
    );
    assert_eq!(manager.len(&stream).await?, 8);
    let remaining = manager
        .range(stream.clone(), IdRange::Minus, IdRange::Plus, None)
        .await?;
    let remaining: Vec<usize> = remaining
        .iter()
        .map(|m| m.message().as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(remaining, (2..10).collect::<Vec<_>>());
    println!("XTRIM MAXLEN ... ok");

    // Everything before message 5 goes: 2, 3, 4.
    assert_eq!(
        manager
            .trim_min_ts(&stream, timestamps[5], TrimMode::Exact)
            .await?,
        3
    );
    assert_eq!(manager.len(&stream).await?, 5);
    let remaining = manager
        .range(stream.clone(), IdRange::Minus, IdRange::Plus, None)
        .await?;
    let remaining: Vec<usize> = remaining
        .iter()
        .map(|m| m.message().as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(remaining, (5..10).collect::<Vec<_>>());
    println!("XTRIM MINID ... ok");

    // IdEx excludes exactly the given entry, on either side of the range.
    let ids: Vec<_> = manager
        .range(stream.clone(), IdRange::Minus, IdRange::Plus, None)
        .await?
        .iter()
        .map(|m| m.message_id())
        .collect();
    assert_eq!(ids.len(), 5); // messages 5..10
    let payloads = |messages: Vec<sea_streamer_redis::RedisMessage>| -> Vec<usize> {
        messages
            .iter()
            .map(|m| m.message().as_str().unwrap().parse().unwrap())
            .collect()
    };
    assert_eq!(
        payloads(
            manager
                .range(
                    stream.clone(),
                    IdRange::IdEx(ids[1]),
                    IdRange::IdEx(ids[3]),
                    None
                )
                .await?
        ),
        vec![7]
    );
    assert_eq!(
        payloads(
            manager
                .range(
                    stream.clone(),
                    IdRange::IdEx(ids[1]),
                    IdRange::IdEx(ids[2]),
                    None
                )
                .await?
        ),
        Vec::<usize>::new()
    );
    assert_eq!(
        payloads(
            manager
                .range(stream.clone(), IdRange::IdEx(ids[3]), IdRange::Plus, None)
                .await?
        ),
        vec![9]
    );
    assert_eq!(
        payloads(
            manager
                .range(stream.clone(), IdRange::Minus, IdRange::IdEx(ids[1]), None)
                .await?
        ),
        vec![5]
    );
    println!("XRANGE IdEx ... ok");

    // The producer's trim takes a u64 end to end: a max_len beyond u32 is a no-op, not an error.
    let producer = streamer.create_generic_producer(Default::default()).await?;
    assert_eq!(
        producer
            .trim_stream_max_len(&stream, u32::MAX as u64 + 1)
            .await?,
        0
    );
    producer.end().await?;
    assert_eq!(manager.len(&stream).await?, 5);
    println!("Producer XTRIM MAXLEN > u32 ... ok");

    // A missing key trims nothing and is not an error.
    assert_eq!(manager.trim_max_len(&missing, 1, TrimMode::Exact).await?, 0);
    assert_eq!(
        manager.trim_min_ts(&missing, now, TrimMode::Approx).await?,
        0
    );

    manager.trim_max_len(&stream, 0, TrimMode::Exact).await?;
    assert_eq!(manager.len(&stream).await?, 0);
    println!("Cleanup ... ok");

    Ok(())
}
