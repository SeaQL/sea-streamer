mod util;
use util::*;

// cargo test --test seek-rewind --features=test,runtime-tokio -- --nocapture
// cargo test --test seek-rewind --no-default-features --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        AutoCommit, AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisStreamer,
    };
    use sea_streamer_runtime::{sleep, timeout};
    use sea_streamer_types::{
        Consumer, ConsumerMode, ConsumerOptions, Producer, SeqPos, StreamKey, Streamer, Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "seek-rewind";
    env_logger::init();

    test(ConsumerMode::RealTime, 1).await?;
    test(ConsumerMode::RealTime, 5).await?;
    test(ConsumerMode::RealTime, 25).await?;

    test(ConsumerMode::Resumable, 1).await?;
    test(ConsumerMode::Resumable, 5).await?;
    test(ConsumerMode::Resumable, 25).await?;

    async fn test(consumer_mode: ConsumerMode, batch_size: usize) -> anyhow::Result<()> {
        println!("ConsumerMode = {consumer_mode:?}; batch_size = {batch_size} ...");

        let options = RedisConnectOptions::default();
        let streamer = RedisStreamer::connect(
            std::env::var("BROKERS_URL")
                .unwrap_or_else(|_| "redis://localhost".to_owned())
                .parse()
                .unwrap(),
            options,
        )
        .await?;

        let now = Timestamp::now_utc();
        let stream = StreamKey::new(format!(
            "{}-{}",
            TEST,
            now.unix_timestamp_nanos() / 1_000_000
        ))?;

        let mut producer = streamer
            .create_producer(stream.clone(), Default::default())
            .await?;

        for i in 0..29 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        let receipt = producer.send("29")?.await?;
        let one_third = *receipt.sequence();
        let point_in_time = *receipt.timestamp();

        sleep(Duration::from_millis(1)).await;

        for i in 30..100 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        let mut options = RedisConsumerOptions::new(consumer_mode);
        options.set_batch_size(batch_size);
        options.set_auto_stream_reset(AutoStreamReset::Earliest);
        options.set_auto_commit(AutoCommit::Disabled); // no pre-fetch
        let mut seeker = streamer.create_consumer(&[stream.clone()], options).await?;

        let seq = consume(&mut seeker, 10).await;
        assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        println!("Stream head ... ok");

        seeker.rewind(SeqPos::Beginning).await?;

        let seq = consume(&mut seeker, 10).await;
        assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        println!("Stream again ... ok");

        seeker.rewind(SeqPos::At(one_third)).await?;

        let seq = consume(&mut seeker, 10).await;
        assert_eq!(seq, [30, 31, 32, 33, 34, 35, 36, 37, 38, 39]);
        println!("Stream rewind ... ok");

        seeker.seek(point_in_time).await?;

        let seq = consume(&mut seeker, 10).await;
        assert_eq!(seq, [30, 31, 32, 33, 34, 35, 36, 37, 38, 39]);
        println!("Stream seek ... ok");

        seeker.rewind(SeqPos::End).await?;
        timeout(Duration::from_millis(5), seeker.next()).await.ok();

        for i in 100..110 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        let seq = consume(&mut seeker, 5).await;
        assert_eq!(seq, [100, 101, 102, 103, 104]);
        let seq = consume(&mut seeker, 5).await;
        assert_eq!(seq, [105, 106, 107, 108, 109]);
        println!("Stream latest ... ok");

        seeker.end().await?;

        println!("End test case.");
        Ok(())
    }

    Ok(())
}
