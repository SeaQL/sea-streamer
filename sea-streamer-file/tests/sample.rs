static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test sample --features=test,runtime-tokio -- --nocapture
// cargo test --test sample --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn sample_1() -> anyhow::Result<()> {
    use sea_streamer_file::{AutoStreamReset, FileConsumerOptions, FileErr, FileId, FileStreamer};
    use sea_streamer_types::{Consumer, Message, StreamErr, StreamKey, Streamer};

    INIT.call_once(env_logger::init);

    let file_id: FileId = "tests/sample-1.ss".parse().unwrap();
    let streamer = FileStreamer::connect(file_id.to_streamer_uri()?, Default::default()).await?;
    let mut options = FileConsumerOptions::default();
    options.set_auto_stream_reset(AutoStreamReset::Earliest);
    let consumer = streamer
        .create_consumer(&[StreamKey::new("event")?], options)
        .await?;

    for i in 1..=22 {
        let mess = consumer.next().await?;
        assert_eq!(mess.sequence(), i);
    }
    let err = consumer.next().await;
    assert!(matches!(err, Err(StreamErr::Backend(FileErr::StreamEnded))));

    Ok(())
}
