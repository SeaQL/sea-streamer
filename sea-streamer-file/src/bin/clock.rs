use anyhow::{anyhow, Result};
use sea_streamer_file::{ByteSink, Bytes, FileId, FileSink, WriteFrom, DEFAULT_FILE_SIZE_LIMIT};
use std::time::Duration;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "File path")]
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
    let mut stream = FileSink::new(file, WriteFrom::End, DEFAULT_FILE_SIZE_LIMIT).await?;

    for i in 0..u64::MAX {
        stream.write(Bytes::Bytes(format!("{i}\n").into_bytes()))?;
        tokio::time::sleep(interval).await;
    }

    Ok(())
}
