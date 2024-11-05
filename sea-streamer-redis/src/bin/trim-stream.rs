use anyhow::Result;
use clap::Parser;
use sea_streamer_redis::RedisStreamer;
use sea_streamer_types::{StreamUrl, Streamer};

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
        help = "Trim the stream down to this number of items (not exact)"
    )]
    maxlen: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream, maxlen } = Args::parse();
    let stream_key = stream.stream_key()?;

    let streamer = RedisStreamer::connect(stream.streamer(), Default::default()).await?;
    let producer = streamer.create_generic_producer(Default::default()).await?;

    match producer.trim(&stream_key, maxlen).await {
        Ok(trimmed) => log::info!("XTRIM {stream_key} trimmed {trimmed} entries"),
        Err(err) => log::error!("{err:?}"),
    }

    Ok(())
}
