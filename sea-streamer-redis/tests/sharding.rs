mod util;
use util::*;

// cargo test --test sharding --features=test,runtime-tokio -- --nocapture
// cargo test --test sharding --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisProducerOptions,
        RedisStreamer, RoundRobinSharder,
    };
    use sea_streamer_runtime::sleep;
    use sea_streamer_types::{
        ConsumerMode, ConsumerOptions, Producer, StreamKey, Streamer, StreamerUri, Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "sharding";
    const SHARDS: u32 = 3;
    env_logger::init();

    test(ConsumerMode::RealTime).await?;
    test(ConsumerMode::Resumable).await?;

    async fn test(mode: ConsumerMode) -> anyhow::Result<()> {
        println!("ConsumerMode = {mode:?} ...");

        let options = RedisConnectOptions::default();
        let streamer = RedisStreamer::connect(
            std::env::var("BROKERS_URL")
                .unwrap_or_else(|_| "redis://localhost".to_owned())
                .parse::<StreamerUri>()
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

        let mut options = RedisProducerOptions::default();
        options.set_sharder(RoundRobinSharder::new(SHARDS));
        let mut producer = streamer.create_producer(stream.clone(), options).await?;

        let mut sequence = 0;
        for i in 0..10 {
            let message = format!("{i}");
            let receipt = producer.send(message)?.await?;
            assert_eq!(receipt.stream_key(), &stream);
            // should always increase
            assert!(receipt.sequence() > &sequence);
            sequence = *receipt.sequence();
            assert_eq!(receipt.shard_id().id(), i % SHARDS as u64);
            sleep(Duration::from_millis(1)).await;
        }

        println!("Stream to shards ... ok");

        let mut options = RedisConsumerOptions::new(mode);
        options.set_auto_stream_reset(AutoStreamReset::Earliest);

        let mut full = streamer.create_consumer(&[stream.clone()], options).await?;

        let mut seq = consume(&mut full, 10).await?;
        seq.sort();
        assert_eq!(seq, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        println!("Stream from shards ... ok");

        for i in 10..20 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;
        let mut seq = consume(&mut full, 10).await?;
        seq.sort();
        assert_eq!(seq, [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);

        println!("Stream more shards ... ok");

        producer.end().await?;
        full.end().await?;

        println!("End test case.");
        Ok(())
    }

    Ok(())
}
