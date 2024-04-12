use anyhow::Result;
use sea_streamer_kafka::KafkaStreamer;
use sea_streamer_types::{Producer, StreamUrl, Streamer};
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key, i.e. try `kafka://localhost:9092/hello`",
        env = "STREAM_URL"
    )]
    stream: StreamUrl,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::parse();

    let streamer = KafkaStreamer::connect(stream.streamer(), Default::default()).await?;
    let producer = streamer
        .create_producer(stream.stream_key()?, Default::default())
        .await?;

    for i in 0..1000 {
        let message = format!("{{\"hello\": {i}}}");
        producer.send(message)?;
    }

    streamer.disconnect().await?;

    Ok(())
}
