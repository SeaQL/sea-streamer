use anyhow::Result;
use clap::Parser;
use sea_streamer_redis::{IdRange, RedisStreamer};
use sea_streamer_types::{Buffer, Message, StreamKey, Streamer, StreamerUri};

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
    let streams = manager.scan("0").await?.streams;
    log::info!("{:#?}", streams);

    let messages = manager
        .range(
            StreamKey::new(&streams[0])?,
            IdRange::Minus,
            IdRange::Plus,
            Some(1),
        )
        .await?;

    if messages.is_empty() {
        log::info!("No messages");
    }

    for msg in messages {
        log::info!(
            "[{}] [{}] {}",
            msg.timestamp(),
            msg.stream_key(),
            msg.message().as_str().unwrap()
        );
    }

    Ok(())
}
