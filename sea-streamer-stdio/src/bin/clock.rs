use anyhow::{anyhow, Result};
use clap::Parser;
use sea_streamer_stdio::StdioStreamer;
use sea_streamer_types::{Producer, StreamKey, Streamer, StreamerUri};
use std::time::Duration;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Stream key")]
    stream: StreamKey,
    #[clap(long, value_parser = parse_duration, help = "Period of the clock. e.g. 1s, 100ms")]
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

    let Args { stream, interval } = Args::parse();

    let streamer = StdioStreamer::connect(StreamerUri::zero(), Default::default()).await?;
    let producer = streamer.create_producer(stream, Default::default()).await?;
    let mut tick: u64 = 0;

    loop {
        producer.send(format!(r#"{{ "tick": {tick} }}"#))?;
        tick += 1;
        tokio::time::sleep(interval).await;
    }
}
