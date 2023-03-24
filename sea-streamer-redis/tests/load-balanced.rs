mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

#[allow(unused_macros)]
macro_rules! flush {
    () => {
        std::io::Write::flush(&mut std::io::stdout()).unwrap()
    };
}

// cargo test --test load-balanced --features=test,runtime-tokio -- --nocapture
// cargo test --test load-balanced --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn load_balance() -> anyhow::Result<()> {
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

    const TEST: &str = "balanced-1";
    INIT.call_once(env_logger::init);

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
            "{}-{}",
            TEST,
            now.unix_timestamp_nanos() / 1_000_000
        ))?;

        let mut producer = streamer
            .create_producer(stream.clone(), Default::default())
            .await?;

        for i in 0..5 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        let mut options = RedisConsumerOptions::new(ConsumerMode::LoadBalanced);
        options.set_auto_stream_reset(AutoStreamReset::Earliest);
        options.set_auto_commit(auto_commit);
        options.set_auto_commit_interval(Duration::from_secs(0));
        // set a smaller batch size, otherwise one would take more than it can handle
        options.set_batch_size(1);

        let alpha = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        // separate them by 1ms so they must have different consumer ids
        sleep(Duration::from_millis(1)).await;

        let beta = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        // we want to test the default group and consumer option,
        // which the 2 consumers should be in the same group,
        // but with two different consumer ids.

        assert!(alpha.group_id().is_some());
        assert_eq!(alpha.group_id(), beta.group_id());
        assert!(alpha.consumer_id().is_some());
        assert!(beta.consumer_id().is_some());
        assert_ne!(alpha.consumer_id(), beta.consumer_id());

        for i in 5..10 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        // multiplex the two streams into one
        let (sender, messages) = unbounded();

        {
            let sender = sender.clone();
            spawn_task(async move {
                while let Ok(msg) = alpha.next().await {
                    if sender.send(("a", msg)).is_err() {
                        break;
                    }
                    // simulate time needed to process a message
                    sleep(Duration::from_millis(1)).await;
                }
            });
        }
        {
            let sender = sender.clone();
            spawn_task(async move {
                let sender = sender.clone();
                while let Ok(msg) = beta.next().await {
                    if sender.send(("b", msg)).is_err() {
                        break;
                    }
                    sleep(Duration::from_millis(1)).await;
                }
            });
        }

        let messages: Vec<_> = messages.into_stream().take(10).collect().await;

        let mut numbers = Vec::new();
        let (mut a_count, mut b_count) = (0, 0);

        for (who, msg) in messages {
            match who {
                "a" => a_count += 1,
                "b" => b_count += 1,
                _ => unreachable!(),
            }
            let num = msg.message().as_str().unwrap().parse::<usize>().unwrap();
            numbers.push(num);
            log::debug!("[{who}] {num}");
        }

        // they may be received out-of-order
        numbers.sort();
        assert_eq!(numbers, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_ne!(a_count, 0);
        assert_ne!(b_count, 0);

        // example:
        // [a] 0
        // [b] 1
        // [a] 2
        // [b] 3
        // [a] 4
        // [b] 5
        // [a] 6
        // [b] 7
        // [a] 8
        // [b] 9

        println!("End test case.");
        Ok(())
    }

    Ok(())
}

// cargo test --test load-balanced --features=test,runtime-tokio -- --nocapture
// cargo test --test load-balanced --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn failover() -> anyhow::Result<()> {
    use sea_streamer_redis::{
        AutoCommit, AutoStreamReset, RedisConnectOptions, RedisConsumerOptions, RedisStreamer,
    };
    use sea_streamer_types::{
        ConsumerGroup, ConsumerMode, ConsumerOptions, Producer, StreamKey, Streamer, Timestamp,
    };
    use std::time::Duration;

    const TEST: &str = "balanced-2";
    INIT.call_once(env_logger::init);

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
            "{}-{}",
            TEST,
            now.unix_timestamp_nanos() / 1_000_000
        ))?;

        let mut producer = streamer
            .create_producer(stream.clone(), Default::default())
            .await?;

        let mut options = RedisConsumerOptions::new(ConsumerMode::LoadBalanced);
        options.set_consumer_group(ConsumerGroup::new(stream.name()))?;
        options.set_auto_stream_reset(AutoStreamReset::Earliest);
        options.set_auto_commit(auto_commit);
        options.set_auto_commit_interval(Duration::from_secs(0));
        options.set_auto_claim_idle(Duration::from_secs(1));
        options.set_auto_claim_interval(Some(Duration::from_secs(1)));
        options.set_batch_size(1);

        let mut alpha = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;
        let alpha_id = alpha.consumer_id().unwrap().to_owned();

        for i in 0..5 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        print!("Stream alpha ...");
        flush!();
        let seq = consume(&mut alpha, 5).await;
        assert_eq!(seq, [0, 1, 2, 3, 4]);
        println!(" ok");

        let mut beta = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        assert!(beta.consumer_id().is_some());
        assert_ne!(alpha.consumer_id(), beta.consumer_id());

        for i in 5..10 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;

        print!("Stream beta ...");
        flush!();
        let seq = consume_and_ack(&mut beta, 5).await;
        assert_eq!(seq, [5, 6, 7, 8, 9]);
        println!(" ok");

        print!("Commit beta ...");
        flush!();
        beta.commit()?.await?;
        println!(" ok");

        print!("Abort alpha ...");
        flush!();
        // end alpha without ACKing anything
        alpha.end().await?;
        println!(" ok");

        print!("Stream claim ...");
        flush!();
        // there are no new messages, so after XREAD timed out it will try XCLAIM
        let seq = consume_and_ack(&mut beta, 5).await;
        assert_eq!(seq, [0, 1, 2, 3, 4]);
        println!(" ok");

        for i in 10..15 {
            let message = format!("{i}");
            producer.send(message)?;
        }

        producer.flush().await?;
        beta.commit()?.await?;

        options.set_consumer_id(alpha_id);
        let mut alpha = streamer
            .create_consumer(&[stream.clone()], options.clone())
            .await?;

        print!("Resume alpha ...");
        flush!();
        let seq = consume(&mut alpha, 2).await;
        // alpha starts streaming from where the group is at
        assert_eq!(seq, [10, 11]);
        println!(" ok");

        print!("Resume beta ...");
        flush!();
        let seq = consume(&mut beta, 3).await;
        // alpha is idle, so beta steps in
        assert_eq!(seq, [12, 13, 14]);
        println!(" ok");

        println!("End test case.");
        Ok(())
    }

    Ok(())
}
