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
        format::{self, Beacon, Beacons, Checksum, HeaderV1, ShortString},
        Bytes, FileSink, FileSource, ReadFrom, WriteFrom, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{Buffer, MessageHeader, OwnedMessage, ShardId, StreamKey, Timestamp};

    const TEST: &str = "loopback";

    let now = Timestamp::now_utc();
    let path = temp_file(format!("{}-{}", TEST, now.unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{path}");

    let mut sink = FileSink::new(&path, WriteFrom::Beginning, DEFAULT_FILE_SIZE_LIMIT).await?;
    let mut source = FileSource::new(&path, ReadFrom::Beginning).await?;

    let bytes = Bytes::from_bytes(vec![1, 2, 3, 4]);
    bytes.clone().write_to(&mut sink)?;
    let read = Bytes::read_from(&mut source, bytes.len()).await?;
    assert_eq!(read, bytes);

    let bytes = Bytes::from_bytes(vec![5, 6, 7, 8]);
    bytes.write_to(&mut sink)?;

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
    let read = format::MessageHeader::read_from(&mut source).await?;
    assert_eq!(mess_header, read);

    let mut message = format::Message {
        message: OwnedMessage::new(mess_header.0.clone(), "123456789".into_bytes()),
        checksum: 0,
    };
    let size = message.size();
    assert_eq!(size, message.clone().write_to(&mut sink)?.0);
    let read = format::Message::read_from(&mut source).await?;
    message.checksum = 0x4C06;
    assert_eq!(message, read);

    let beacon = Beacons {
        remaining_messages_bytes: 1234,
        items: vec![
            Beacon {
                header: mess_header.0.clone(),
                running_checksum: Checksum(5678),
            },
            Beacon {
                header: mess_header.0.clone(),
                running_checksum: Checksum(9876),
            },
        ],
    };
    let size = beacon.size();
    assert_eq!(size, beacon.clone().write_to(&mut sink)?);
    let read = Beacons::read_from(&mut source).await?;
    assert_eq!(beacon, read);

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn seek() -> anyhow::Result<()> {
    use sea_streamer_file::{
        Bytes, FileSink, FileSource, ReadFrom, WriteFrom, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{SeqPos, Timestamp};

    const TEST: &str = "seek";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let path = temp_file(format!("{}-{}", TEST, now.unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{path}");

    let mut sink = FileSink::new(&path, WriteFrom::Beginning, DEFAULT_FILE_SIZE_LIMIT).await?;
    let mut source = FileSource::new(&path, ReadFrom::Beginning).await?;

    let bytes = Bytes::from_bytes(vec![1, 2, 3, 4]);
    bytes.clone().write_to(&mut sink)?;
    let read = Bytes::read_from(&mut source, bytes.len()).await?;
    assert_eq(read.bytes(), bytes.bytes());

    let bytes = Bytes::from_bytes(vec![5, 6, 7, 8]);
    bytes.write_to(&mut sink)?;

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
    let read = Bytes::read_from(&mut source, 4).await?;
    assert_eq(read.bytes(), vec![7, 8, 9, 10]);

    source.seek(SeqPos::End).await?;
    Bytes::Bytes(vec![11, 12]).write_to(&mut sink)?;
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
        format::Beacons, Bytes, FileSink, FileSource, MessageSource, ReadFrom, WriteFrom,
        DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::Timestamp;

    const TEST: &str = "beacon";
    INIT.call_once(env_logger::init);

    let now = Timestamp::now_utc();
    let path = temp_file(format!("{}-{}", TEST, now.unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{path}");

    let mut sink = FileSink::new(&path, WriteFrom::Beginning, DEFAULT_FILE_SIZE_LIMIT).await?;
    let source = FileSource::new(&path, ReadFrom::Beginning).await?;
    let mut source = MessageSource::new_with(source, 0, 12);

    Bytes::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).write_to(&mut sink)?;
    Beacons {
        items: Vec::new(),
        remaining_messages_bytes: 0x88,
    }
    .write_to(&mut sink)?;
    Bytes::Bytes(vec![13, 14, 15, 16, 17]).write_to(&mut sink)?;
    Beacons {
        items: Vec::new(),
        remaining_messages_bytes: 0x99,
    }
    .write_to(&mut sink)?;
    Bytes::Bytes(vec![18, 19, 20]).write_to(&mut sink)?;

    let read = Bytes::read_from(&mut source, 8).await?;
    assert_eq!(read.bytes(), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let read = Bytes::read_from(&mut source, 8).await?;
    assert_eq!(read.bytes(), vec![9, 10, 11, 12, 13, 14, 15, 16]);
    let read = Bytes::read_from(&mut source, 4).await?;
    assert_eq!(read.bytes(), vec![17, 18, 19, 20]);

    Ok(())
}

#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn messages() -> anyhow::Result<()> {
    use sea_streamer_file::{format::RunningChecksum, MessageSink, MessageSource};
    use sea_streamer_types::{
        Buffer, MessageHeader, OwnedMessage, SeqPos, ShardId, StreamKey, Timestamp,
    };

    const TEST: &str = "messages";
    INIT.call_once(env_logger::init);

    let path =
        temp_file(format!("{}-{}", TEST, now().unix_timestamp_nanos() / 1_000_000).as_str())?;
    println!("{path}");

    let mut sink = MessageSink::new(&path, 640, 1024 * 1024).await?;
    let mut source = MessageSource::new(&path).await?;

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
    assert_eq!(source.beacons()[0].running_checksum, running_checksum.crc());
    dbg!(source.beacons());

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
