mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test loopback --features=test,runtime-tokio -- --nocapture
// cargo test --test loopback --features=test,runtime-async-std -- --nocapture

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn loopback() -> anyhow::Result<()> {
    use sea_streamer_file::{
        format::{self, Beacon, Checksum, HeaderV1, Marker, ShortString},
        AsyncFile, Bytes, FileSink, FileSource, ReadFrom, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{Buffer, MessageHeader, OwnedMessage, ShardId, StreamKey, Timestamp};

    const TEST: &str = "loopback";

    let now = Timestamp::now_utc();
    let path = temp_file(format!("{}-{}", TEST, now.unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{path}");

    let mut sink = FileSink::new(
        AsyncFile::new_ow(path.clone()).await?,
        DEFAULT_FILE_SIZE_LIMIT,
    )?;
    let mut source = FileSource::new(path.clone(), ReadFrom::Beginning).await?;

    let bytes = Bytes::from_bytes(vec![1, 2, 3, 4]);
    bytes.clone().write_to(&mut sink)?;
    sink.flush(1).await?;

    let read = Bytes::read_from(&mut source, bytes.len()).await?;
    assert_eq!(read, bytes);

    let bytes = Bytes::from_bytes(vec![5, 6, 7, 8]);
    bytes.write_to(&mut sink)?;
    sink.flush(2).await?;

    let read = Bytes::read_from(&mut source, 2).await?;
    assert_eq!(read.bytes(), vec![5, 6]);

    let read = Bytes::read_from(&mut source, 2).await?;
    assert_eq!(read.bytes(), vec![7, 8]);

    let timestamp = now.replace_millisecond(now.millisecond())?;

    assert!(
        ShortString::new("Lorem ipsum dolor sit amet, consectetur adipiscing elit".to_owned())
            .is_ok()
    );
    assert!(
        ShortString::new("Lorem ipsum dolor sit amet, consectetur adipiscing elit, ".to_owned() +
        "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam" +
        ", quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in").is_err()
    );

    let header = HeaderV1 {
        file_name: "hello".to_owned(),
        created_at: timestamp,
        beacon_interval: 12345,
    };
    let size = HeaderV1::size();
    assert_eq!(size, header.clone().write_to(&mut sink)?);
    sink.flush(3).await?;
    let read = HeaderV1::read_from(&mut source).await?;
    assert_eq!(header, read);

    let mess_header = format::MessageHeader(MessageHeader::new(
        StreamKey::new("stream_key")?,
        ShardId::new(1122334455667788),
        123456789101112,
        timestamp,
    ));
    let size = mess_header.size();
    assert_eq!(size, mess_header.clone().write_to(&mut sink)?);
    sink.flush(4).await?;
    let read = format::MessageHeader::read_from(&mut source).await?;
    assert_eq!(mess_header, read);

    let mut message = format::Message {
        message: OwnedMessage::new(mess_header.0.clone(), "123456789".into_bytes()),
        checksum: 0,
    };
    let size = message.size();
    assert_eq!(size, message.clone().write_to(&mut sink)?.0);
    sink.flush(5).await?;
    let read = format::Message::read_from(&mut source).await?;
    message.checksum = 0x4C06;
    assert_eq!(message, read);

    let beacon = Beacon {
        remaining_messages_bytes: 1234,
        items: vec![
            Marker {
                header: mess_header.0.clone(),
                running_checksum: Checksum(5678),
            },
            Marker {
                header: mess_header.0.clone(),
                running_checksum: Checksum(9876),
            },
        ],
    };
    let size = beacon.size();
    assert_eq!(size, beacon.clone().write_to(&mut sink)?);
    sink.flush(6).await?;
    let read = Beacon::read_from(&mut source).await?;
    assert_eq!(beacon, read);

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn file() -> anyhow::Result<()> {
    use sea_streamer_file::{
        AsyncFile, Bytes, FileSink, FileSource, ReadFrom, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{SeqPos, Timestamp};

    const TEST: &str = "file";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let path = temp_file(format!("{}-{}", TEST, now.unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{path}");

    let mut sink = FileSink::new(
        AsyncFile::new_ow(path.clone()).await?,
        DEFAULT_FILE_SIZE_LIMIT,
    )?;
    let mut source = FileSource::new(path.clone(), ReadFrom::Beginning).await?;

    let bytes = Bytes::from_bytes(vec![1, 2, 3, 4]);
    bytes.clone().write_to(&mut sink)?;
    sink.flush(1).await?;
    let read = Bytes::read_from(&mut source, bytes.len()).await?;
    assert_eq(read.bytes(), bytes.bytes());

    let bytes = Bytes::from_bytes(vec![5, 6, 7, 8]);
    bytes.write_to(&mut sink)?;
    sink.flush(2).await?;

    let read = Bytes::read_from(&mut source, 2).await?;
    assert_eq(read.bytes(), vec![5, 6]);

    let read = Bytes::read_from(&mut source, 2).await?;
    assert_eq(read.bytes(), vec![7, 8]);

    source.seek(SeqPos::Beginning).await?;
    let read = Bytes::read_from(&mut source, 6).await?;
    assert_eq(read.bytes(), vec![1, 2, 3, 4, 5, 6]);

    source.seek(SeqPos::At(4)).await?;
    let read = Bytes::read_from(&mut source, 4).await?;
    assert_eq(read.bytes(), vec![5, 6, 7, 8]);

    source.seek(SeqPos::At(6)).await?;
    Bytes::Bytes(vec![9, 10]).write_to(&mut sink)?;
    sink.flush(3).await?;
    let read = Bytes::read_from(&mut source, 4).await?;
    assert_eq(read.bytes(), vec![7, 8, 9, 10]);

    source.seek(SeqPos::End).await?;
    Bytes::Bytes(vec![11, 12]).write_to(&mut sink)?;
    sink.flush(4).await?;
    let read = Bytes::read_from(&mut source, 2).await?;
    assert_eq(read.bytes(), vec![11, 12]);

    fn assert_eq(a: Vec<u8>, b: Vec<u8>) {
        assert_eq!(a, b);
        println!("{}", format!("{a:?}").replace('\n', ""));
    }

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn beacon() -> anyhow::Result<()> {
    use sea_streamer_file::{
        format::{Beacon, Header},
        AsyncFile, Bytes, FileErr, FileSink, FileSourceType, MessageSource, StreamMode,
        DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{SeqPos, Timestamp};

    const TEST: &str = "beacon";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let path = temp_file(format!("{}-{}", TEST, now.unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{path}");

    let mut sink = FileSink::new(
        AsyncFile::new_ow(path.clone()).await?,
        DEFAULT_FILE_SIZE_LIMIT,
    )?;
    let header = Header {
        file_name: path.to_string(),
        created_at: now,
        beacon_interval: 128,
    };
    header.clone().write_to(&mut sink)?;
    sink.flush(1).await?;
    let mut source = MessageSource::new(path.clone(), StreamMode::LiveReplay).await?;

    // A beacon immediately after the header
    Beacon {
        items: Vec::new(),
        remaining_messages_bytes: 0,
    }
    .write_to(&mut sink)?;

    // The empty beacon is 7 bytes, so we have 121 bytes for data
    Bytes::Bytes(vec![
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
        49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
        72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94,
        95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113,
        114, 115, 116, 117, 118, 119, 120, 121,
    ])
    .write_to(&mut sink)?;

    Beacon {
        items: Vec::new(),
        remaining_messages_bytes: 0,
    }
    .write_to(&mut sink)?;

    // Another chunk of data
    Bytes::Bytes(vec![
        101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118,
        119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136,
        137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154,
        155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172,
        173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190,
        191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208,
        209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221,
    ])
    .write_to(&mut sink)?;

    Beacon {
        items: Vec::new(),
        remaining_messages_bytes: 2, // ! tricky
    }
    .write_to(&mut sink)?;

    // Finally, some residue
    Bytes::Bytes(vec![222, 223, 224]).write_to(&mut sink)?;
    sink.flush(2).await?;

    let read = Bytes::read_from(&mut source, 8).await?;
    assert_eq!(read.bytes(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let read = Bytes::read_from(&mut source, 8).await?;
    assert_eq!(read.bytes(), vec![9, 10, 11, 12, 13, 14, 15, 16]);
    let read = Bytes::read_from(&mut source, 4).await?;
    assert_eq!(read.bytes(), vec![17, 18, 19, 20]);
    _ = Bytes::read_from(&mut source, 128 - 20 - 7).await?;
    let read = Bytes::read_from(&mut source, 8).await?;
    assert_eq!(read.bytes(), vec![101, 102, 103, 104, 105, 106, 107, 108]);

    // Rewind the file and read again
    assert_eq!(2, source.rewind(SeqPos::At(2)).await?);
    let read = Bytes::read_from(&mut source, 7).await?;
    assert_eq!(read.bytes(), vec![101, 102, 103, 104, 105, 106, 107]);
    let read = Bytes::read_from(&mut source, 1).await?;
    assert_eq!(read.bytes(), vec![108]);

    // Rewind the file and switch to dead mode
    assert_eq!(1, source.rewind(SeqPos::At(1)).await?);
    source.switch_to(FileSourceType::FileReader).await?;
    let read = Bytes::read_from(&mut source, 4).await?;
    assert_eq!(read.bytes(), vec![1, 2, 3, 4]);
    let read = Bytes::read_from(&mut source, 4).await?;
    assert_eq!(read.bytes(), vec![5, 6, 7, 8]);
    _ = Bytes::read_from(&mut source, 128 - 8 - 7 + 128 - 7).await?;
    let read = Bytes::read_from(&mut source, 3).await?;
    assert_eq!(read.bytes(), vec![222, 223, 224]);
    // Now it should error
    let error = Bytes::read_from(&mut source, 1).await.err().unwrap();
    assert!(matches!(error, FileErr::NotEnoughBytes));

    // Rewind to the 2nd beacon, and skip two remaining bytes
    assert_eq!(3, source.rewind(SeqPos::At(3)).await?);
    let read = Bytes::read_from(&mut source, 1).await?;
    assert_eq!(read.bytes(), vec![224]);
    let error = Bytes::read_from(&mut source, 1).await.err().unwrap();
    assert!(matches!(error, FileErr::NotEnoughBytes));

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn sink() -> anyhow::Result<()> {
    use sea_streamer_file::{MessageSink, MessageSource, StreamMode};
    use sea_streamer_types::{
        Buffer, Message, MessageHeader, OwnedMessage, ShardId, StreamKey, Timestamp,
    };

    const TEST: &str = "sink";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let path = temp_file(format!("{}-{}", TEST, millis_of(&now)).as_str())?;
    println!("{path}");

    let stream_key = StreamKey::new("hello")?;
    let content = "How are you? I am fine thank you. The weather is nice today, isn't it?";
    let message = |i| {
        let header = MessageHeader::new(stream_key.clone(), ShardId::new(0), i, now);
        OwnedMessage::new(header, content.into_bytes())
    };

    let mut bea_int = 128;
    while bea_int <= 1024 {
        println!("Beacon Interval = {bea_int}");

        let mut sink = MessageSink::new(path.clone(), bea_int, 1024 * 1024).await?;
        for i in 0..10 {
            sink.write(message(i)).await?;
        }
        sink.end(false).await?;
        std::mem::drop(sink);

        let mut sink = MessageSink::append(path.clone(), bea_int, 1024 * 1024).await?;
        for i in 10..20 {
            sink.write(message(i)).await?;
        }
        sink.end(true).await?;
        std::mem::drop(sink);

        let mut sink = MessageSink::append(path.clone(), bea_int, 1024 * 1024).await?;
        for i in 20..30 {
            sink.write(message(i)).await?;
        }
        sink.end(true).await?;
        std::mem::drop(sink);

        let mut source = MessageSource::new(path.clone(), StreamMode::Replay).await?;
        for i in 0..30 {
            let m = source.next().await?;
            assert_eq!(m.message.stream_key(), stream_key);
            assert_eq!(m.message.sequence(), i);
        }
        bea_int *= 2;
        println!("Sink ... ok");
    }

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn rewind() -> anyhow::Result<()> {
    use sea_streamer_file::{format::RunningChecksum, MessageSink, MessageSource, StreamMode};
    use sea_streamer_types::{
        Buffer, MessageHeader, OwnedMessage, SeqPos, ShardId, StreamKey, Timestamp,
    };

    const TEST: &str = "rewind";
    INIT.call_once(env_logger::init);

    let path = temp_file(format!("{}-{}", TEST, millis_of(&now())).as_str())?;
    println!("{path}");

    let mut sink = MessageSink::new(path.clone(), 640, 1024 * 1024).await?;
    let mut source = MessageSource::new(path.clone(), StreamMode::LiveReplay).await?;

    let stream_key = StreamKey::new("hello")?;
    let mut running_checksum = RunningChecksum::new();

    let header = MessageHeader::new(stream_key.clone(), ShardId::new(2), 1, now());
    let world = OwnedMessage::new(header, "world".into_bytes());
    running_checksum.update(sink.write(world.clone()).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &world);

    let payload = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, ".to_owned() +
    "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam" +
    ", quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in"
    ).into_bytes();

    let header = MessageHeader::new(stream_key.clone(), ShardId::new(2), 2, now());
    let message = OwnedMessage::new(header, payload.clone());
    running_checksum.update(sink.write(message.clone()).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    let header = MessageHeader::new(stream_key.clone(), ShardId::new(2), 3, now());
    let message = OwnedMessage::new(header, payload.clone());
    running_checksum.update(sink.write(message.clone()).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    // There should be a beacon here
    assert_eq!(source.beacon().0, 1);
    assert_eq!(
        source.beacon().1[0].running_checksum,
        running_checksum.crc()
    );

    let header = MessageHeader::new(stream_key.clone(), ShardId::new(2), 4, now());
    let message = OwnedMessage::new(header, payload.clone());
    running_checksum.update(sink.write(message.clone()).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    // Try rewind
    assert_eq!(1, source.rewind(SeqPos::At(1)).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    // Fast forward
    assert_eq!(1, source.rewind(SeqPos::End).await?);

    // Let's make a message spanning multiple beacons
    let header = MessageHeader::new(stream_key.clone(), ShardId::new(2), 5, now());
    let message = OwnedMessage::new(header, vec![1; 768]);
    running_checksum.update(sink.write(message.clone()).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    let header = MessageHeader::new(stream_key.clone(), ShardId::new(2), 6, now());
    let message = OwnedMessage::new(header, payload.clone());
    running_checksum.update(sink.write(message.clone()).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    // Now, seeking to 2 would cause a slippage to 3
    assert_eq!(3, source.rewind(SeqPos::At(2)).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    // Rewind everything
    assert_eq!(0, source.rewind(SeqPos::Beginning).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &world);

    // Fast forward
    assert_eq!(3, source.rewind(SeqPos::End).await?);
    let header = MessageHeader::new(stream_key.clone(), ShardId::new(2), 7, now());
    let message = OwnedMessage::new(header, payload.clone());
    running_checksum.update(sink.write(message.clone()).await?);
    let read = source.next().await?;
    assert_eq(&read.message, &message);

    fn assert_eq(a: &OwnedMessage, b: &OwnedMessage) {
        assert_eq!(a, b);
        dbg!(a.header());
    }

    fn now() -> Timestamp {
        let now = Timestamp::now_utc();
        let now = now.replace_millisecond(now.millisecond()).unwrap();
        now
    }

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn source() -> anyhow::Result<()> {
    use sea_streamer_file::{
        BeaconReader, FileErr, MessageSink, MessageSource, SeekErr, SeekTarget, StreamMode,
    };
    use sea_streamer_types::{
        Buffer, Message, MessageHeader, OwnedMessage, ShardId, StreamKey, Timestamp,
    };

    const TEST: &str = "source";
    INIT.call_once(env_logger::init);

    for t in 0..2 {
        let path = temp_file(format!("{}-{}", TEST, millis_of(&now())).as_str())?;
        println!("{path}");

        let suffix = match t {
            0 => 0,   // baseline; there is no beacons
            1 => 250, // there should be 5 beacons
            _ => unreachable!(),
        };
        let mut sink = MessageSink::new(path.clone(), 640, 1024 * 1024).await?;
        let mut source = MessageSource::new(path.clone(), StreamMode::LiveReplay).await?;

        let stream_key = StreamKey::new("hello")?;
        let shard_id = ShardId::new(0);

        let start = now().unix_timestamp() * 1000;
        for i in 1..=10 {
            let ts = Timestamp::from_unix_timestamp_nanos((start + i as i64) as i128 * 1_000_000)?;
            let header = MessageHeader::new(stream_key.clone(), shard_id, i, ts);
            let message = OwnedMessage::new(
                header,
                format!(
                    "message-{}{}",
                    i,
                    String::from_utf8(vec![b' '; suffix]).unwrap()
                )
                .into_bytes(),
            );
            sink.write(message).await?;
        }

        source
            .seek(&stream_key, &shard_id, SeekTarget::SeqNo(0))
            .await?;
        let m = source.next().await?;
        assert_eq!(m.message.header().sequence(), &1);

        assert_eq!(
            source.max_beacons(),
            match t {
                0 => 0,
                1 => 5,
                _ => unreachable!(),
            }
        );

        // Seek by SeqNo
        source
            .seek(&stream_key, &shard_id, SeekTarget::SeqNo(1))
            .await?;
        let m = source.next().await?;
        assert_eq!(m.message.header().sequence(), &1);

        source
            .seek(&stream_key, &shard_id, SeekTarget::SeqNo(5))
            .await?;
        for i in 5..=10 {
            let m = source.next().await?;
            assert_eq!(m.message.header().stream_key(), &stream_key);
            assert_eq!(m.message.header().shard_id(), &shard_id);
            assert_eq!(m.message.header().sequence(), &i);
            assert_eq!(
                m.message.message().as_str().unwrap().trim_end(),
                format!("message-{i}").as_str()
            );
        }

        source
            .seek(&stream_key, &shard_id, SeekTarget::SeqNo(10))
            .await?;
        let m = source.next().await?;
        assert_eq!(m.message.header().sequence(), &10);

        let err = source
            .seek(&stream_key, &shard_id, SeekTarget::SeqNo(11))
            .await;
        assert!(matches!(err, Err(FileErr::SeekErr(SeekErr::OutOfBound))));
        println!("Seek by SeqNo ... ok");

        // Seek by Timestamp
        let ts = Timestamp::from_unix_timestamp_nanos((start + 0) as i128 * 1_000_000)?;
        source
            .seek(&stream_key, &shard_id, SeekTarget::Timestamp(ts))
            .await?;
        let m = source.next().await?;
        assert_eq!(m.message.header().sequence(), &1);

        let ts = Timestamp::from_unix_timestamp_nanos((start + 5) as i128 * 1_000_000)?;
        source
            .seek(&stream_key, &shard_id, SeekTarget::Timestamp(ts))
            .await?;
        for i in 6..=10 {
            let m = source.next().await?;
            assert_eq!(m.message.header().stream_key(), &stream_key);
            assert_eq!(m.message.header().shard_id(), &shard_id);
            assert_eq!(m.message.header().sequence(), &i);
        }
        let ts = Timestamp::from_unix_timestamp_nanos((start + 10) as i128 * 1_000_000)?;
        let err = source
            .seek(&stream_key, &shard_id, SeekTarget::Timestamp(ts))
            .await;
        assert!(matches!(err, Err(FileErr::SeekErr(SeekErr::OutOfBound))));
        println!("Seek by Timestamp ... ok");
    }

    fn now() -> Timestamp {
        Timestamp::now_utc()
    }

    Ok(())
}
