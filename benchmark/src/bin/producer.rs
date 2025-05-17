use anyhow::Result;
use clap::Parser;
use sea_streamer::{Producer, SeaProducer, SeaStreamer, StreamUrl, Streamer};
use std::time::Duration;

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

    for i in 0..100_000 {
        let message = format!(
            "The this the message payload {i:0>5}: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo"
        );
        producer.send(message)?;
        if i % 1000 == 0 {
            tokio::time::sleep(Duration::from_nanos(1)).await;
        }
    }

    producer.end().await?; // flush

    Ok(())
}
