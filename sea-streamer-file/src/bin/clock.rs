use anyhow::{anyhow, Result};
use sea_streamer_file::{FileId, MessageSink, DEFAULT_BEACON_INTERVAL, DEFAULT_FILE_SIZE_LIMIT};
use sea_streamer_types::{MessageHeader, OwnedMessage, ShardId, StreamKey, Timestamp};
use std::time::Duration;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "Stream to this file")]
    file: FileId,
    #[structopt(long, parse(try_from_str = parse_duration), help = "Period of the clock. e.g. 1s, 100ms")]
    interval: Duration,
}

fn parse_duration(src: &str) -> Result<Duration> {
    if let Some(s) = src.strip_suffix("ns") {
        Ok(Duration::from_nanos(s.parse()?))
    } else if let Some(s) = src.strip_suffix("us") {
        Ok(Duration::from_micros(s.parse()?))
    } else if let Some(s) = src.strip_suffix("ms") {
        Ok(Duration::from_millis(s.parse()?))
    } else if let Some(s) = src.strip_suffix('s') {
        Ok(Duration::from_secs(s.parse()?))
    } else if let Some(s) = src.strip_suffix('m') {
        Ok(Duration::from_secs(s.parse::<u64>()? * 60))
    } else {
        Err(anyhow!("Failed to parse {} as Duration", src))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { file, interval } = Args::from_args();
    let mut sink = MessageSink::new(
        file.clone(),
        DEFAULT_BEACON_INTERVAL,
        DEFAULT_FILE_SIZE_LIMIT,
    )
    .await?;
    let stream_key = StreamKey::new("hello")?;
    let shard = ShardId::new(0);

    for i in 0..u64::MAX {
        let header = MessageHeader::new(stream_key.clone(), shard, i, Timestamp::now_utc());
        let message = OwnedMessage::new(header, format!("hello-{i}").into_bytes());
        sink.write(message).await?;
        tokio::time::sleep(interval).await;
    }

    Ok(())
}
