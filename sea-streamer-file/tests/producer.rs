mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test producer --features=test,runtime-tokio -- --nocapture
// cargo test --test producer --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn producer() -> anyhow::Result<()> {
    use std::time::Duration;

    use sea_streamer_file::{
        AutoStreamReset, FileConnectOptions, FileConsumerOptions, FileErr, FileStreamer,
    };
    use sea_streamer_runtime::sleep;
    use sea_streamer_types::{
        Buffer, Consumer, Message, Producer, SeqNo, SharedMessage, StreamErr, StreamKey, Streamer,
        Timestamp,
    };

    INIT.call_once(env_logger::init);
    run("multi-producer", false).await?;
    run("shared-producer", true).await?;

    async fn run(test: &'static str, shared: bool) -> anyhow::Result<()> {
        let now = Timestamp::now_utc();
        let file_id = temp_file(format!("{}-{}", test, millis_of(&now)).as_str())?;
        println!("{file_id}");
        let stream_key = StreamKey::new("hello")?;

        let mut options = FileConnectOptions::default();
        options.set_beacon_interval(1024)?;
        options.set_end_with_eos(true);
        let streamer = FileStreamer::connect(file_id.to_streamer_uri()?, options.clone()).await?;

        let producer = streamer
            .create_producer(stream_key.clone(), Default::default())
            .await?;
        let secondary = if shared {
            producer.clone()
        } else {
            streamer
                .create_producer(stream_key.clone(), Default::default())
                .await?
        };
        let consumer = streamer
            .create_consumer(&[stream_key.clone()], Default::default())
            .await?;

        let check = |m: SharedMessage, i: SeqNo| {
            let h = m.header();
            assert_eq!(h.stream_key(), &stream_key);
            assert_eq!(h.sequence(), &i);
            let num: SeqNo = m.message().as_str().unwrap().parse().unwrap();
            assert_eq!(num, i);
        };

        for i in 1..25 {
            let mess = format!("{}", i);
            producer.send(mess)?;
            check(consumer.next().await?, i);
        }
        println!("Send ... ok");

        for i in (25..75).step_by(2) {
            let mess = format!("{}", i);
            producer.send(mess)?;
            let mess = format!("{}", i + 1);
            secondary.send(mess)?;
            check(consumer.next().await?, i);
            check(consumer.next().await?, i + 1);
        }
        println!("Mux ... ok");

        // this should not cause any issue
        std::mem::drop(secondary);

        for i in 75..125 {
            let mess = format!("{}", i);
            producer.send(mess)?;
        }
        for i in 75..125 {
            check(consumer.next().await?, i);
        }
        println!("Drop ... ok");

        streamer.disconnect().await?;

        assert!(matches!(
            producer.send("hello"),
            Err(StreamErr::Backend(FileErr::ProducerEnded))
        ));
        assert!(matches!(
            consumer.next().await,
            Err(StreamErr::Backend(FileErr::StreamEnded))
        ));

        std::mem::drop(producer);
        std::mem::drop(consumer);
        println!("Disconnect ... ok");
        sleep(Duration::from_millis(1)).await;

        let streamer = FileStreamer::connect(file_id.to_streamer_uri()?, options).await?;
        let mut producer = streamer
            .create_producer(stream_key.clone(), Default::default())
            .await?;
        let mut consumer_options = FileConsumerOptions::default();
        consumer_options.set_auto_stream_reset(AutoStreamReset::Earliest);

        for i in 125..150 {
            let mess = format!("{}", i);
            producer.send(mess)?;
        }
        // we don't truncate files, so while we're overwriting the remaining bytes
        // the file is actually in a 'corrupted' state
        producer.flush().await?;
        println!("Reconnect ... ok");

        let consumer = streamer
            .create_consumer(&[stream_key.clone()], consumer_options)
            .await?;
        for i in 1..150 {
            check(consumer.next().await?, i);
        }
        println!("Append stream ... ok");

        Ok(())
    }

    Ok(())
}
