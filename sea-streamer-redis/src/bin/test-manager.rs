use anyhow::Result;
use clap::Parser;
use sea_streamer_redis::RedisStreamer;
use sea_streamer_types::{Streamer, StreamerUri};

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI",
        default_value = "redis://localhost",
        env = "STREAMER_URI"
    )]
    streamer: StreamerUri,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { streamer } = Args::parse();

    let streamer = RedisStreamer::connect(streamer, Default::default()).await?;
    let mut manager = streamer.create_manager().await?;
    log::info!("{:#?}", manager.scan("0").await?);

    Ok(())
}
