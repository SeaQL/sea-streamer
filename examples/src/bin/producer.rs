use anyhow::Result;
use sea_streamer::{Producer, SeaProducer, SeaStreamer, StreamKey, Streamer};
use std::time::Duration;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "Stream key")]
    stream: StreamKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::from_args();

    let streamer = SeaStreamer::connect(
        std::env::var("STREAMER_URL")
            .unwrap_or_else(|_| "kafka://localhost:9092".to_owned())
            .parse()?,
        Default::default(),
    )
    .await?;

    let producer: SeaProducer = streamer.create_producer(stream, Default::default()).await?;

    for tick in 0..10 {
        let message = format!(r#""tick {tick}""#);
        println!("{message}");
        producer.send(message)?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    producer.flush(Duration::from_secs(10)).await?;

    Ok(())
}
