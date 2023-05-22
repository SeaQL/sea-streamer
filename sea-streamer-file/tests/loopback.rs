mod util;
use util::*;

// cargo test --test loopback --features=test,runtime-tokio -- --nocapture
// cargo test --test loopback --features=test,runtime-async-std -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn main() -> anyhow::Result<()> {
    use sea_streamer_file::{
        format::HeaderV1, Bytes, FileSink, FileSource, ReadFrom, WriteFrom, DEFAULT_FILE_SIZE_LIMIT,
    };
    use sea_streamer_types::Timestamp;

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

    let created_at = now.replace_millisecond(0)?;
    let header = HeaderV1 {
        file_name: "hello".to_owned(),
        created_at,
    };
    header.clone().write_to(&mut sink)?;
    let read = HeaderV1::read_from(&mut source).await?;
    assert_eq!(header, read);

    Ok(())
}
