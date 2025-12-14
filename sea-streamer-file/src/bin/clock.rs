use anyhow::{Result, anyhow};
use clap::Parser;
use sea_streamer_file::{FileId, FileStreamer};
use sea_streamer_types::{Producer, StreamKey, Streamer};
use std::time::Duration;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Stream to this file")]
    file: FileId,
    #[clap(long, value_parser = parse_duration, help = "Period of the clock. e.g. 1s, 100ms")]
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

    let stream_key = StreamKey::new("clock")?;
    let streamer = FileStreamer::connect(file.to_streamer_uri()?, Default::default()).await?;
    let producer = streamer
        .create_producer(stream_key, Default::default())
        .await?;

    for i in 0..u64::MAX {
        producer.send(format!("tick-{i}"))?;
        tokio::time::sleep(interval).await;
    }

    producer.end().await?;

    Ok(())
}
