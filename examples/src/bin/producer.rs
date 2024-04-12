use anyhow::Result;
use sea_streamer::{Producer, SeaProducer, SeaStreamer, StreamUrl, Streamer};
use std::time::Duration;
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key, i.e. try `kafka://localhost:9092/my_topic`",
        env = "STREAM_URL"
    )]
    stream: StreamUrl,
}

#[cfg_attr(feature = "runtime-tokio", tokio::main)]
#[cfg_attr(feature = "runtime-async-std", async_std::main)]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::parse();

    let streamer = SeaStreamer::connect(stream.streamer(), Default::default()).await?;

    let producer: SeaProducer = streamer
        .create_producer(stream.stream_key()?, Default::default())
        .await?;

    for tick in 0..100 {
        let message = format!(r#""tick {tick}""#);
        eprintln!("{message}");
        producer.send(message)?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    producer.end().await?; // flush

    Ok(())
}
