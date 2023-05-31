mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test consumer --features=test,runtime-tokio -- --nocapture
// cargo test --test consumer --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn consumer() -> anyhow::Result<()> {
    use anyhow::Context;
    use sea_streamer_file::{
        AutoStreamReset, FileConsumerOptions, FileStreamer, MessageSink, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{
        export::futures::TryStreamExt, Buffer, Consumer, Message, MessageHeader, OwnedMessage,
        ShardId, SharedMessage, StreamKey, Streamer, Timestamp,
    };

    const TEST: &str = "consumer";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let file_id =
        temp_file(format!("{}-{}", TEST, now.unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{file_id}");
    let stream_key = StreamKey::new("hello")?;
    let shard = ShardId::new(1);

    let message = |i: u64| -> OwnedMessage {
        let header = MessageHeader::new(stream_key.clone(), shard, i, Timestamp::now_utc());
        OwnedMessage::new(header, format!("{}-{}", stream_key.name(), i).into_bytes())
    };
    let check = |i: u64, mess: SharedMessage| {
        assert_eq!(mess.header().stream_key(), &stream_key);
        assert_eq!(mess.header().shard_id(), &shard);
        assert_eq!(mess.header().sequence(), &i);
        assert_eq!(
            mess.message().as_str().unwrap(),
            format!("{}-{}", stream_key.name(), i)
        );
    };

    let streamer = FileStreamer::connect(file_id.to_streamer_uri()?, Default::default()).await?;

    let mut sink = MessageSink::new(
        file_id.clone(),
        1024, // 1KB
        DEFAULT_FILE_SIZE_LIMIT,
    )
    .await?;

    for i in 0..50 {
        sink.write(message(i)).await?;
    }

    let mut options = FileConsumerOptions::default();
    options.set_auto_stream_reset(AutoStreamReset::Earliest);
    let mut earliest = streamer
        .create_consumer(&[stream_key.clone()], options.clone())
        .await?;

    for i in 0..25 {
        check(i, earliest.next().await?);
    }
    {
        let mut stream = earliest.stream();
        for i in 25..50 {
            check(i, stream.try_next().await?.context("Never ends")?);
        }
    }
    println!("Stream from earliest ... ok");

    options.set_auto_stream_reset(AutoStreamReset::Latest);
    let latest = streamer
        .create_consumer(&[stream_key.clone()], options)
        .await?;

    for i in 50..100 {
        sink.write(message(i)).await?;
    }
    for i in 50..100 {
        check(i, earliest.next().await?);
        check(i, latest.next().await?);
    }
    println!("Stream from latest ... ok");

    Ok(())
}
