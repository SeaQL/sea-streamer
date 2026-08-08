//! Regression tests for a `RealTime` consumer subscribed to multiple streams.
//!
//! A shard that has never delivered used to keep `id == None` and re-send `$`
//! on every `XREAD`, re-anchoring to the current tail. When a busy sibling
//! stream keeps the blocking read returning, a quiet stream's messages fell
//! into the gap between calls and were silently lost. These tests pin down that
//! behaviour deterministically by exploiting `RealTime`'s zero-capacity pre-fetch
//! channel: the read loop won't issue the next `XREAD` until the current message
//! is drained via `next()`, so we can produce onto a quiet stream *after* a busy
//! stream's read has returned but *before* the next read re-anchors.

// cargo test --test realtime-multi --features=test,runtime-tokio -- --nocapture
// cargo test --test realtime-multi --no-default-features --features=test,runtime-smol -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-smol", smol_potat::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_redis::{AutoStreamReset, RedisConsumer, RedisConsumerOptions, RedisStreamer};
    use sea_streamer_runtime::{sleep, timeout};
    use sea_streamer_types::{
        Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, Producer, StreamKey, Streamer,
        Timestamp,
    };
    use std::{collections::HashSet, time::Duration};

    env_logger::init();

    async fn connect() -> anyhow::Result<RedisStreamer> {
        let mut options = sea_streamer_redis::RedisConnectOptions::default();
        options.set_enable_cluster(false);
        #[cfg(feature = "nanosecond-timestamp")]
        options.set_timestamp_format(sea_streamer_redis::TimestampFormat::UnixTimestampNanos);
        Ok(RedisStreamer::connect(
            std::env::var("BROKERS_URL")
                .unwrap_or_else(|_| "redis://localhost".to_owned())
                .parse()
                .unwrap(),
            options,
        )
        .await?)
    }

    fn stream_key(name: &str) -> anyhow::Result<StreamKey> {
        let millis = Timestamp::now_utc().unix_timestamp_nanos() / 1_000_000;
        Ok(StreamKey::new(format!("realtime-multi-{millis}-{name}"))?)
    }

    // Receive one message payload, failing (rather than hanging) if starved.
    async fn recv(consumer: &mut RedisConsumer) -> anyhow::Result<String> {
        let mess = timeout(Duration::from_secs(5), consumer.next())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for a message"))??;
        Ok(mess.message().as_str()?.to_owned())
    }

    // Collect the next `n` payloads into a set (order across streams is not guaranteed).
    async fn recv_set(consumer: &mut RedisConsumer, n: usize) -> anyhow::Result<HashSet<String>> {
        let mut set = HashSet::new();
        for _ in 0..n {
            set.insert(recv(consumer).await?);
        }
        Ok(set)
    }

    let latest = || {
        let mut options = RedisConsumerOptions::new(ConsumerMode::RealTime);
        options.set_auto_stream_reset(AutoStreamReset::Latest);
        options
    };

    // A busy stream keeps the shared XREAD returning; a quiet stream that has
    // never delivered must still be caught.
    async fn fast_starves_slow(
        streamer: &RedisStreamer,
        options: RedisConsumerOptions,
    ) -> anyhow::Result<()> {
        let fast = stream_key("fast")?;
        let slow = stream_key("slow")?;
        let producer = streamer.create_generic_producer(Default::default()).await?;
        let mut consumer = streamer
            .create_consumer(&[fast.clone(), slow.clone()], options)
            .await?;

        // Ensure the first XREAD is in flight (anchored while both streams are empty).
        sleep(Duration::from_millis(100)).await;

        // The busy stream delivers -> XREAD returns and the loop parks on the
        // zero-capacity handoff, having *not* yet issued the next XREAD.
        producer.send_to(&fast, "f0".to_owned())?.await?;
        // Let that read return before writing to the quiet stream, so the quiet
        // message is not swept up by the same in-flight read.
        sleep(Duration::from_millis(100)).await;

        // Quiet message lands before the next XREAD is issued.
        producer.send_to(&slow, "s0".to_owned())?.await?;

        // Draining f0 releases the loop -> next XREAD. Pre-fix this re-anchors the
        // slow shard past s0, losing it forever.
        assert_eq!(recv(&mut consumer).await?, "f0");

        // Give the read loop a reason to return regardless, then require s0.
        producer.send_to(&fast, "f1".to_owned())?.await?;
        let got = recv_set(&mut consumer, 2).await?;
        assert!(got.contains("s0"), "quiet stream was starved (got {got:?})");
        assert!(got.contains("f1"));

        consumer.end().await?;
        println!("fast_starves_slow ... ok");
        Ok(())
    }

    // The "8 keys" scenario: one busy stream and several quiet ones.
    async fn many_quiet_streams(
        streamer: &RedisStreamer,
        options: RedisConsumerOptions,
    ) -> anyhow::Result<()> {
        let fast = stream_key("fast")?;
        let quiet: Vec<StreamKey> = (0..3)
            .map(|i| stream_key(&format!("quiet{i}")))
            .collect::<anyhow::Result<_>>()?;
        let mut keys = vec![fast.clone()];
        keys.extend(quiet.iter().cloned());

        let producer = streamer.create_generic_producer(Default::default()).await?;
        let mut consumer = streamer.create_consumer(&keys, options).await?;
        sleep(Duration::from_millis(100)).await;

        producer.send_to(&fast, "f0".to_owned())?.await?;
        sleep(Duration::from_millis(100)).await;

        // One message onto each quiet stream, all before the next XREAD.
        let mut expected = HashSet::new();
        for (i, key) in quiet.iter().enumerate() {
            let payload = format!("q{i}");
            producer.send_to(key, payload.clone())?.await?;
            expected.insert(payload);
        }

        assert_eq!(recv(&mut consumer).await?, "f0");
        producer.send_to(&fast, "f1".to_owned())?.await?;
        expected.insert("f1".to_owned());

        let got = recv_set(&mut consumer, expected.len()).await?;
        assert_eq!(got, expected, "some quiet streams were starved");

        consumer.end().await?;
        println!("many_quiet_streams ... ok");
        Ok(())
    }

    // Pinning the tail must anchor at each shard's *own* last id, not `0-0`:
    // a `Latest` consumer must not replay a stream's pre-existing history.
    async fn latest_skips_history(
        streamer: &RedisStreamer,
        options: RedisConsumerOptions,
    ) -> anyhow::Result<()> {
        let a = stream_key("hist-a")?;
        let b = stream_key("hist-b")?;
        let producer = streamer.create_generic_producer(Default::default()).await?;

        // Pre-existing history on both streams.
        for i in 0..3 {
            producer.send_to(&a, format!("a-old-{i}"))?.await?;
            producer.send_to(&b, format!("b-old-{i}"))?.await?;
        }

        let mut consumer = streamer
            .create_consumer(&[a.clone(), b.clone()], options)
            .await?;
        sleep(Duration::from_millis(100)).await;

        // Only these new messages should arrive.
        producer.send_to(&a, "a-new".to_owned())?.await?;
        producer.send_to(&b, "b-new".to_owned())?.await?;

        let got = recv_set(&mut consumer, 2).await?;
        assert_eq!(
            got,
            HashSet::from(["a-new".to_owned(), "b-new".to_owned()]),
            "Latest replayed history instead of anchoring at the tail"
        );

        consumer.end().await?;
        println!("latest_skips_history ... ok");
        Ok(())
    }

    let streamer = connect().await?;
    println!("Connect Streamer ... ok");

    fast_starves_slow(&streamer, latest()).await?;
    many_quiet_streams(&streamer, latest()).await?;
    latest_skips_history(&streamer, latest()).await?;

    println!("End test case.");
    Ok(())
}
