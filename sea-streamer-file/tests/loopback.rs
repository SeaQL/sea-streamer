mod util;
use util::*;

// cargo test --test loopback --features=test,runtime-tokio -- --nocapture
// cargo test --test loopback --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_file::{
        format::{self, HeaderV1, ShortString},
        Bytes, FileSink, FileSource, ReadFrom, WriteFrom, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::{MessageHeader, ShardId, StreamKey, Timestamp};

    const TEST: &str = "loopback";
    env_logger::init();

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

    let timestamp = now.replace_microsecond(0)?;

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
    header.clone().write_to(&mut sink)?;
    let read = HeaderV1::read_from(&mut source).await?;
    assert_eq!(header, read);

    let mess_header = format::MessageHeader(MessageHeader::new(
        StreamKey::new("stream_key")?,
        ShardId::new(1122334455667788),
        123456789101112,
        timestamp,
    ));
    mess_header.clone().write_to(&mut sink)?;
    let read = format::MessageHeader::read_from(&mut source).await?;
    assert_eq!(mess_header, read);

    Ok(())
}
