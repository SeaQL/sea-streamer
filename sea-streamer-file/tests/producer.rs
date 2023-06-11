mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test producer --features=test,runtime-tokio -- --nocapture
// cargo test --test producer --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn producer() -> anyhow::Result<()> {
    use sea_streamer_file::{FileConnectOptions, FileErr, FileStreamer};
    use sea_streamer_types::{
        Buffer, Consumer, Message, Producer, SeqNo, SharedMessage, StreamErr, StreamKey, Streamer,
        Timestamp,
    };

    const TEST: &str = "producer";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let file_id = temp_file(format!("{}-{}", TEST, millis_of(&now)).as_str())?;
    println!("{file_id}");
    let stream_key = StreamKey::new("hello")?;

    let mut options = FileConnectOptions::default();
    options.set_beacon_interval(1024)?;
    let streamer = FileStreamer::connect(file_id.to_streamer_uri()?, options).await?;

    let producer = streamer
        .create_producer(stream_key.clone(), Default::default())
        .await?;
    let secondary = streamer
        .create_producer(stream_key.clone(), Default::default())
        .await?;
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

    for i in (25..75).step_by(2) {
        let mess = format!("{}", i);
        producer.send(mess)?;
        let mess = format!("{}", i + 1);
        secondary.send(mess)?;
        check(consumer.next().await?, i);
        check(consumer.next().await?, i + 1);
    }

    for i in 75..125 {
        let mess = format!("{}", i);
        producer.send(mess)?;
    }

    for i in 75..125 {
        check(consumer.next().await?, i);
    }

    streamer.disconnect().await?;

    assert!(matches!(
        producer.send("hello")?.await,
        Err(StreamErr::Backend(FileErr::ProducerEnded))
    ));

    Ok(())
}
