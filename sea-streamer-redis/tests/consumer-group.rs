mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test consumer-group --features=test,runtime-tokio -- --nocapture
// cargo test --test consumer-group --no-default-features --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn consumer_group() -> anyhow::Result<()> {
    use flume::unbounded;
    use sea_streamer_redis::{
        AutoCommit, AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisStreamer,
    };
    use sea_streamer_runtime::{sleep, spawn_task};
    use sea_streamer_types::{
        export::futures::stream::StreamExt, Buffer, Consumer, ConsumerMode, ConsumerOptions,
        Message, Producer, StreamKey, Streamer, Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "group-1";
    INIT.call_once(env_logger::init);

    test(false).await?;
    sleep(Duration::from_millis(1)).await;
    test(true).await?;

    async fn test(mkstream: bool) -> anyhow::Result<()> {
        println!("mkstream = {mkstream:?} ...");

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
        println!("stream = {stream}");

        let mut producer = streamer
            .create_producer(stream.clone(), Default::default())
            .await?;

        let mut options = RedisConsumerOptions::new(ConsumerMode::LoadBalanced);
        options.set_mkstream(mkstream);
        options.set_auto_stream_reset(AutoStreamReset::Earliest);

        let mut consumer = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        if !mkstream {
            assert!(consumer.next().await.is_err());
        }

        for i in 0..5 {
            let message = format!("{i}");
            producer.send(message)?;
        }
        producer.flush().await?;

        if mkstream {
            let seq = consume(&mut consumer, 5).await?;
            assert_eq!(seq, [0, 1, 2, 3, 4]);
        }

        Ok(())
    }

    Ok(())
}
