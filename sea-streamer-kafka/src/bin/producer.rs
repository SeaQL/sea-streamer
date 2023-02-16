use anyhow::Result;
use sea_streamer_kafka::KafkaStreamer;
use sea_streamer_types::{Producer, StreamKey, Streamer};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "Output stream")]
    output: StreamKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { output } = Args::from_args();

    let streamer = KafkaStreamer::connect(
        std::env::var("BROKERS_URL")
            .unwrap_or_else(|_| "localhost:9092".to_owned())
            .parse()
            .unwrap(),
        Default::default(),
    )
    .await?;
    let producer = streamer.create_producer(output, Default::default()).await?;

    for i in 0..100_000 {
        let message = format!("{{\"hello\": {i}}}");
        producer.send(message)?;
    }

    streamer.disconnect().await?;

    Ok(())
}
