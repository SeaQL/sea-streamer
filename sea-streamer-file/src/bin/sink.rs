use anyhow::{anyhow, Result};
use clap::Parser;
use sea_streamer_file::{FileId, MessageSink, DEFAULT_BEACON_INTERVAL, DEFAULT_FILE_SIZE_LIMIT};
use sea_streamer_types::{MessageHeader, OwnedMessage, ShardId, StreamKey, Timestamp};
use std::time::Duration;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Stream to this file")]
    file: FileId,
    #[clap(long, parse(try_from_str = parse_duration), help = "Period of the clock. e.g. 1s, 100ms")]
    interval: Duration,
}

fn parse_duration(src: &str) -> Result<Duration> {
    if let Some(s) = src.strip_suffix("ms") {
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

    let Args { file, interval } = Args::parse();
    let mut sink = MessageSink::new(
        file.clone(),
        DEFAULT_BEACON_INTERVAL,
        DEFAULT_FILE_SIZE_LIMIT,
    )
    .await?;
    let stream_key = StreamKey::new("clock")?;
    let shard = ShardId::new(0);

    for i in 0..u64::MAX {
        let header = MessageHeader::new(stream_key.clone(), shard, i, Timestamp::now_utc());
        let message = OwnedMessage::new(header, format!("tick-{i}").into_bytes());
        sink.write(message)?;
        tokio::time::sleep(interval).await;
    }

    sink.flush().await?;

    Ok(())
}
