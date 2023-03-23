mod util;
use util::*;

// cargo test --test realtime --features=test,runtime-tokio -- --nocapture
// cargo test --test realtime --no-default-features --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisStreamer,
    };
    use sea_streamer_runtime::sleep;
    use sea_streamer_types::{
        ConsumerMode, ConsumerOptions, Producer, ShardId, StreamKey, Streamer, Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "realtime";
    env_logger::init();
    test(false).await?;
    test(true).await?;

    async fn test(enable_cluster: bool) -> anyhow::Result<()> {
        println!("Enable Cluster = {enable_cluster} ...");

        let mut options = RedisConnectOptions::default();
        options.set_enable_cluster(enable_cluster);
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
        let zero = ShardId::new(0);

        let mut producer = streamer
            .create_producer(stream.clone(), Default::default())
            .await?;

        let mut sequence = 0;
        for i in 0..5 {
            let message = format!("{i}");
            let receipt = producer.send(message)?.await?;
            assert_eq!(receipt.stream_key(), &stream);
            // should always increase
            assert!(receipt.sequence() > &sequence);
            sequence = *receipt.sequence();
            assert_eq!(receipt.shard_id(), &zero);
        }

        let mut options = RedisConsumerOptions::new(ConsumerMode::RealTime);
        options.set_auto_stream_reset(AutoStreamReset::Latest);

        let mut half = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        sleep(Duration::from_millis(5)).await;

        for i in 5..10 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        options.set_auto_stream_reset(AutoStreamReset::Earliest);
        let mut full = streamer.create_consumer(&[stream.clone()], options).await?;

        let seq = consume(&mut half, 5).await;
        assert_eq!(seq, [5, 6, 7, 8, 9]);
        println!("Stream latest ... ok");

        let seq = consume(&mut full, 10).await;
        assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        println!("Stream history ... ok");

        for i in 10..13 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;
        half.end().await?;

        let seq = consume(&mut full, 2).await;
        assert_eq!(seq, [10, 11]);

        for i in 13..15 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.end().await?;

        let seq = consume(&mut full, 3).await;
        assert_eq!(seq, [12, 13, 14]);

        println!("Stream realtime ... ok");

        full.end().await?;

        println!("End test case.");
        Ok(())
    }

    Ok(())
}
