mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test resumable --features=test,runtime-tokio -- --nocapture
// cargo test --test resumable --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn immediate_and_delayed() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        AutoCommit, AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisStreamer,
    };
    use sea_streamer_types::{
        ConsumerGroup, ConsumerMode, ConsumerOptions, Producer, ShardId, StreamKey, Streamer,
        Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "resumable-1";
    INIT.call_once(env_logger::init);
    test(AutoCommit::Immediate).await?;
    test(AutoCommit::Delayed).await?;

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
        options.set_consumer_group(ConsumerGroup::new(format!("{}a", stream.name())))?;
        options.set_auto_stream_reset(AutoStreamReset::Latest);
        options.set_auto_commit(auto_commit);
        // zero delay
        options.set_auto_commit_delay(Duration::from_secs(0));

        let mut half = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        options.set_consumer_group(ConsumerGroup::new(format!("{}b", stream.name())))?;
        options.set_auto_stream_reset(AutoStreamReset::Earliest);
        let mut full = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        let seq = consume(&mut full, 5).await;
        assert_eq!(seq, [0, 1, 2, 3, 4]);
        println!("Stream history ... ok");

        for i in 5..10 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        // now end the consumer, before it consume any new messages
        // commit and end
        full.end().await?;

        let seq = consume(&mut half, 5).await;
        assert_eq!(seq, [5, 6, 7, 8, 9]);
        println!("Stream latest ... ok");

        // resume from last committed
        let mut full = streamer.create_consumer(&[stream.clone()], options).await?;
        let seq = consume(&mut full, 5).await;
        assert_eq!(seq.len(), 5, "Should have at least 5 items");
        assert_ne!(seq[0], 0, "Should start from anything other than 0");
        println!("{seq:?}");
        println!("Stream resume ... ok");

        println!("End test case.");
        Ok(())
    }

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn rolling_and_disabled() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        AutoCommit, AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisStreamer,
    };
    use sea_streamer_types::{
        export::futures::StreamExt, Buffer, Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions,
        Message, Producer, StreamKey, Streamer, Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "resumable-2";
    INIT.call_once(env_logger::init);

    test(AutoCommit::Rolling).await?;
    test(AutoCommit::Disabled).await?;

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

        let producer = streamer
            .create_producer(stream.clone(), Default::default())
            .await?;

        let mut options = RedisConsumerOptions::new(ConsumerMode::Resumable);
        options.set_consumer_group(ConsumerGroup::new(format!("{}c", stream.name())))?;
        options.set_auto_stream_reset(AutoStreamReset::Latest);
        options.set_auto_commit(auto_commit);
        options.set_auto_commit_interval(Duration::from_secs(0));

        // just to create the stream
        producer.send("-1")?.await?;

        let mut consumer = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        for i in 0..5 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush_once().await?;

        // read 5
        let mess: Vec<_> = consumer.stream().take(5).collect().await;

        for (i, msg) in mess.into_iter().enumerate() {
            let msg = msg?;
            let num: usize = msg.message().as_str()?.parse()?;
            assert_eq!(i, num);
            // but commit only 4
            if i < 4 {
                consumer.ack(&msg)?;
            }
        }
        println!("Stream latest ... ok");

        for i in 5..10 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        if auto_commit == AutoCommit::Rolling {
            // should not allow
            assert!(consumer.commit().await.is_err());
            // tick, should receive the ACK
            consumer.next().await?;
            // tick, should commit
            consumer.next().await?;
        } else {
            // manually commit
            consumer.commit().await?;
        }

        // no need to end properly
        std::mem::drop(consumer);

        // new consumer
        let mut consumer = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        let seq = consume(&mut consumer, 6).await;
        assert_eq!(seq, [4, 5, 6, 7, 8, 9]);
        println!("Stream resume ... ok");

        println!("End test case.");

        Ok(())
    }

    Ok(())
}
