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
        end_of_stream, AutoStreamReset, FileConsumerOptions, FileErr, FileStreamer, MessageSink,
        DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{
        export::futures::TryStreamExt, Buffer, Consumer, Message, MessageHeader, OwnedMessage,
        ShardId, SharedMessage, StreamErr, StreamKey, Streamer, Timestamp,
    };

    const TEST: &str = "consumer";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let file_id = temp_file(format!("{}-{}", TEST, millis_of(&now)).as_str())?;
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

    sink.write(end_of_stream()).await?;
    let ended = |e| matches!(e, Err(StreamErr::Backend(FileErr::StreamEnded)));
    assert!(ended(earliest.next().await));
    assert!(ended(latest.next().await));

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn demux() -> anyhow::Result<()> {
    use sea_streamer_file::{
        query_streamer, AutoStreamReset, FileConsumerOptions, FileStreamer, MessageSink,
        DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{
        Buffer, Consumer, Message, MessageHeader, OwnedMessage, ShardId, SharedMessage, StreamKey,
        Streamer, Timestamp,
    };

    const TEST: &str = "demux";
    INIT.call_once(env_logger::init);

    run("a", AutoStreamReset::Earliest).await?;
    run("b", AutoStreamReset::Latest).await?;

    async fn run(suffix: &'static str, stream_reset: AutoStreamReset) -> anyhow::Result<()> {
        println!("AutoStreamReset = {stream_reset:?}");
        let now = Timestamp::now_utc();
        let file_id = temp_file(format!("{}-{}-{}", TEST, millis_of(&now), suffix).as_str())?;
        println!("{file_id}");
        let cat_key = StreamKey::new("cat")?;
        let dog_key = StreamKey::new("dog")?;
        let shard = ShardId::new(1);

        let cat = |i: u64| -> OwnedMessage {
            let header = MessageHeader::new(cat_key.clone(), shard, i, Timestamp::now_utc());
            OwnedMessage::new(header, format!("{}", i).into_bytes())
        };
        let dog = |i: u64| -> OwnedMessage {
            let header = MessageHeader::new(dog_key.clone(), shard, i, Timestamp::now_utc());
            OwnedMessage::new(header, format!("{}", i).into_bytes())
        };
        let check = |i: u64, mess: &SharedMessage| {
            assert_eq!(mess.header().shard_id(), &shard);
            assert_eq!(mess.header().sequence(), &i);
            assert_eq!(mess.message().as_str().unwrap(), format!("{}", i));
        };
        let is_cat = |i: u64, m: SharedMessage| {
            assert_eq!(m.header().stream_key(), &cat_key);
            check(i, &m);
        };
        let is_dog = |i: u64, m: SharedMessage| {
            assert_eq!(m.header().stream_key(), &dog_key);
            check(i, &m);
        };

        let streamer =
            FileStreamer::connect(file_id.to_streamer_uri()?, Default::default()).await?;

        let mut sink = MessageSink::new(
            file_id.clone(),
            1024, // 1KB
            DEFAULT_FILE_SIZE_LIMIT,
        )
        .await?;

        let mut options = FileConsumerOptions::default();
        options.set_auto_stream_reset(stream_reset);

        let cat_stream = streamer
            .create_consumer(&[cat_key.clone()], options.clone())
            .await?;
        let dog_stream = streamer
            .create_consumer(&[dog_key.clone()], options.clone())
            .await?;

        // cat & dog should share the same Streamer
        assert_eq!(query_streamer(&file_id).await.unwrap().len(), 1);

        for i in 0..100 {
            if i % 2 == 0 {
                sink.write(cat(i)).await?;
            } else {
                sink.write(dog(i)).await?;
            }
        }

        for i in 0..100 {
            if i % 2 == 0 {
                is_cat(i, cat_stream.next().await?);
            } else {
                is_dog(i, dog_stream.next().await?);
            }
        }

        println!(" ... ok");
        Ok(())
    }
    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn group() -> anyhow::Result<()> {
    use sea_streamer_file::{
        query_streamer, AutoStreamReset, FileConsumerOptions, FileStreamer, MessageSink,
        DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{
        Buffer, Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions, Message, MessageHeader,
        OwnedMessage, ShardId, SharedMessage, StreamKey, Streamer, Timestamp,
    };

    const TEST: &str = "group";
    INIT.call_once(env_logger::init);

    run("a", AutoStreamReset::Earliest).await?;
    run("b", AutoStreamReset::Latest).await?;

    async fn run(suffix: &'static str, stream_reset: AutoStreamReset) -> anyhow::Result<()> {
        println!("AutoStreamReset = {stream_reset:?}");
        let now = Timestamp::now_utc();
        let file_id = temp_file(format!("{}-{}-{}", TEST, millis_of(&now), suffix).as_str())?;
        println!("{file_id}");
        let stream_key = StreamKey::new("nuts")?;
        let shard = ShardId::new(1);
        let group = ConsumerGroup::new("friends");

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

        let streamer =
            FileStreamer::connect(file_id.to_streamer_uri()?, Default::default()).await?;

        let mut sink = MessageSink::new(
            file_id.clone(),
            1024, // 1KB
            DEFAULT_FILE_SIZE_LIMIT,
        )
        .await?;

        let mut options = FileConsumerOptions::new(ConsumerMode::LoadBalanced);
        options.set_consumer_group(group)?;
        options.set_auto_stream_reset(AutoStreamReset::Earliest);

        let chip = streamer
            .create_consumer(&[stream_key.clone()], options.clone())
            .await?;
        let dale = streamer
            .create_consumer(&[stream_key.clone()], options.clone())
            .await?;

        // chip & dale should share the same Streamer
        assert_eq!(query_streamer(&file_id).await.unwrap().len(), 1);

        for i in 0..100 {
            sink.write(message(i)).await?;
        }

        for i in 0..100 {
            // they share the nuts fairly
            if i % 2 == 0 {
                check(i, chip.next().await?);
            } else {
                check(i, dale.next().await?);
            }
        }

        // here comes the antagonist
        let donald = streamer
            .create_consumer(&[stream_key.clone()], options.clone())
            .await?;

        // for the next batch of nuts
        for i in 100..130 {
            sink.write(message(i)).await?;
        }

        for i in 100..130 {
            match i % 3 {
                0 => check(i, chip.next().await?),
                1 => check(i, dale.next().await?),
                2 => {
                    // some of the nuts are stolen
                    check(i, donald.next().await?)
                }
                _ => unreachable!(),
            }
        }

        println!(" ... ok");
        Ok(())
    }
    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn seek() -> anyhow::Result<()> {
    use sea_streamer_file::{
        AutoStreamReset, FileConsumerOptions, FileStreamer, MessageSink, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{
        Buffer, Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions, Message, MessageHeader,
        OwnedMessage, SeqPos, ShardId, SharedMessage, StreamKey, Streamer, Timestamp,
    };

    const TEST: &str = "seek";
    INIT.call_once(env_logger::init);

    run(false).await?;
    run(true).await?;

    async fn run(grouped: bool) -> anyhow::Result<()> {
        println!("With Group = {grouped}");
        let now = Timestamp::now_utc();
        let file_id = temp_file(format!("{}-{}", TEST, millis_of(&now)).as_str())?;
        println!("{file_id}");
        let stream_key = StreamKey::new("stream")?;
        let shard = ShardId::new(1);
        let group = ConsumerGroup::new("group");

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

        let streamer =
            FileStreamer::connect(file_id.to_streamer_uri()?, Default::default()).await?;

        let mut sink = MessageSink::new(
            file_id.clone(),
            1024, // 1KB
            DEFAULT_FILE_SIZE_LIMIT,
        )
        .await?;

        let mut options = if grouped {
            let mut options = FileConsumerOptions::new(ConsumerMode::LoadBalanced);
            options.set_consumer_group(group)?;
            options
        } else {
            FileConsumerOptions::new(ConsumerMode::RealTime)
        };
        options.set_auto_stream_reset(AutoStreamReset::Earliest);

        let mut solo = streamer
            .create_consumer(&[stream_key.clone()], options.clone())
            .await?;

        for i in 0..100 {
            sink.write(message(i)).await?;
        }
        for i in 0..100 {
            check(i, solo.next().await?);
        }
        println!("Read All ... ok");

        solo.rewind(SeqPos::Beginning).await?;
        for i in 0..100 {
            check(i, solo.next().await?);
        }
        println!("Rewind ... ok");

        solo.rewind(SeqPos::At(25)).await?;
        for i in 25..100 {
            check(i, solo.next().await?);
        }
        println!("Seek to 25 ... ok");

        solo.rewind(SeqPos::At(50)).await?;
        for i in 50..100 {
            check(i, solo.next().await?);
        }
        println!("Seek to 50 ... ok");

        solo.rewind(SeqPos::At(75)).await?;
        for i in 75..100 {
            check(i, solo.next().await?);
        }
        println!("Seek to 75 ... ok");

        solo.rewind(SeqPos::At(99)).await?;
        check(99, solo.next().await?);
        println!("Seek to 99 ... ok");

        Ok(())
    }
    Ok(())
}
