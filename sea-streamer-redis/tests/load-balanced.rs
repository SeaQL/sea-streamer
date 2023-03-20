static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test load-balanced --features=test,runtime-tokio -- --nocapture
// cargo test --test load-balanced --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
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

    const TEST: &str = "load-balanced";
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

        let (sender, messages) = unbounded();

        {
            let sender = sender.clone();
            spawn_task(async move {
                while let Ok(msg) = alpha.next().await {
                    if sender.send(("a", msg)).is_err() {
                        break;
                    }
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
            println!("[{who}] {num}");
        }

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
