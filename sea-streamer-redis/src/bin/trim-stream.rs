use anyhow::Result;
use clap::Parser;
use sea_streamer_redis::RedisStreamer;
use sea_streamer_types::{StreamUrl, Streamer, TIMESTAMP_FORMAT, Timestamp};
use time::PrimitiveDateTime;

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key, i.e. try `redis://localhost/hello`",
        env = "STREAM_URL"
    )]
    stream: StreamUrl,
    #[clap(
        long,
        help = "Trim the stream down to this number of messages (not exact)"
    )]
    max_len: Option<u32>,
    #[clap(
        long,
        help = "Trim all messages with timestamp before this timestamp (not exact)"
    )]
    min_ts: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args {
        stream,
        max_len,
        min_ts,
    } = Args::parse();
    let stream_key = stream.stream_key()?;

    let streamer = RedisStreamer::connect(stream.streamer(), Default::default()).await?;
    let producer = streamer.create_generic_producer(Default::default()).await?;

    if let Some(max_len) = max_len {
        match producer.trim_stream_max_len(&stream_key, max_len).await {
            Ok(trimmed) => log::info!("XTRIM {stream_key} trimmed {trimmed} entries"),
            Err(err) => log::error!("{err:?}"),
        }
    } else if let Some(min_ts) = min_ts {
        let min_ts = parse_timestamp(&min_ts)?;
        match producer.trim_stream_min_ts(&stream_key, min_ts).await {
            Ok(trimmed) => log::info!("XTRIM {stream_key} trimmed {trimmed} entries"),
            Err(err) => log::error!("{err:?}"),
        }
    } else {
        log::warn!("No trim operation.");
    }

    Ok(())
}

fn parse_timestamp(input: &str) -> Result<Timestamp> {
    let ts = PrimitiveDateTime::parse(input, &TIMESTAMP_FORMAT)?;
    Ok(ts.assume_utc())
}
