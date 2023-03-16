mod util;
use util::*;

// cargo test --test resumable --features=test,runtime-tokio -- --nocapture
// cargo test --test resumable --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        AutoCommit, AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisStreamer,
    };
    use sea_streamer_types::{
        ConsumerGroup, ConsumerMode, ConsumerOptions, Producer, ShardId, StreamKey, Streamer,
        Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "resumable";
    env_logger::init();
    test(AutoCommit::Immediate).await?;
    assert!(
        // This test case has a bit of timing element, especially on async-std
        test(AutoCommit::Delayed).await.is_ok() || 
        // So let's try a second time
        test(AutoCommit::Delayed).await.is_ok()
    );

    async fn test(auto_commit: AutoCommit) -> anyhow::Result<()> {
        println!("AutoCommit = {auto_commit:?} ...");

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
            "{}-{}-{}",
            TEST,
            now.unix_timestamp(),
            now.millisecond()
        ))?;
        let zero = ShardId::new(0);

        let producer = streamer
            .create_producer(stream.clone(), Default::default())
            .await?;

        let mut sequence = 0;
        for i in 0..5 {
            let message = format!("{i}");
            let receipt = producer.send(message)?.await?;
            assert_eq!(receipt.stream_key(), &stream);
            assert!(receipt.sequence() > &sequence);
            sequence = *receipt.sequence();
            assert_eq!(receipt.shard_id(), &zero);
        }

        let mut options = RedisConsumerOptions::new(ConsumerMode::Resumable);
        options.set_consumer_group(ConsumerGroup::new(format!("{}1", stream.name())))?;
        options.set_auto_stream_reset(AutoStreamReset::Latest);
        options.set_auto_commit(auto_commit);
        options.set_auto_commit_delay(Duration::from_secs(0));

        let mut half = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        options.set_consumer_group(ConsumerGroup::new(format!("{}2", stream.name())))?;
        options.set_auto_stream_reset(AutoStreamReset::Earliest);
        let mut full = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        let seq = consume(&mut full, 5).await;
        assert_eq!(seq, [0, 1, 2, 3, 4]);
        println!("Stream history ... ok");

        // now end the consumer, before it consume any messages
        // but hopefully it has committed already
        full.end().await?;

        for i in 5..10 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        // add more messages to trigger the next tick
        producer.flush().await?;

        let seq = consume(&mut half, 5).await;
        assert_eq!(seq, [5, 6, 7, 8, 9]);
        println!("Stream latest ... ok");

        // resume from last committed
        let mut full = streamer.create_consumer(&[stream.clone()], options).await?;
        let seq = consume(&mut full, 5).await;
        // there should be at least 5 items, can be more
        assert_eq!(seq.len(), 5);
        // this should start from anything other than 0
        if seq[0] == 0 {
            log::error!("It didn't commit this time. Try again maybe");
            anyhow::bail!(".");
        }
        println!("{seq:?}");
        println!("Stream resume ... ok");

        println!("End test case.");
        Ok(())
    }

    Ok(())
}
