mod util;

// cargo test --test manager --features=test,runtime-tokio -- --nocapture
// cargo test --test manager --features=test,runtime-tokio,nanosecond-timestamp -- --nocapture
// cargo test --test manager --no-default-features --features=test,runtime-smol -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-smol", smol_potat::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_redis::{IdRange, RedisConnectOptions, RedisStreamer, TrimMode};
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

    assert_eq!(manager.xlen(&missing).await?, 0);
    assert_eq!(manager.xlen(&stream).await?, 10);
    println!("XLEN ... ok");

    // Approximate trimming may remove nothing on a tiny stream; it must still succeed.
    manager.trim_max_len(&stream, 8, TrimMode::Approx).await?;
    assert!(manager.xlen(&stream).await? >= 8);

    let before = manager.xlen(&stream).await?;
    assert_eq!(
        manager.trim_max_len(&stream, 8, TrimMode::Exact).await?,
        before - 8
    );
    assert_eq!(manager.xlen(&stream).await?, 8);
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
            .trim_min_id(&stream, timestamps[5], TrimMode::Exact)
            .await?,
        3
    );
    assert_eq!(manager.xlen(&stream).await?, 5);
    let remaining = manager
        .range(stream.clone(), IdRange::Minus, IdRange::Plus, None)
        .await?;
    let remaining: Vec<usize> = remaining
        .iter()
        .map(|m| m.message().as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(remaining, (5..10).collect::<Vec<_>>());
    println!("XTRIM MINID ... ok");

    // A missing key trims nothing and is not an error.
    assert_eq!(manager.trim_max_len(&missing, 1, TrimMode::Exact).await?, 0);
    assert_eq!(
        manager.trim_min_id(&missing, now, TrimMode::Approx).await?,
        0
    );

    manager.trim_max_len(&stream, 0, TrimMode::Exact).await?;
    assert_eq!(manager.xlen(&stream).await?, 0);
    println!("Cleanup ... ok");

    Ok(())
}
