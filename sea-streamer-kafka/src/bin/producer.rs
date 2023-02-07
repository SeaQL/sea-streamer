use anyhow::Result;
use sea_streamer::{Message, Producer, Sendable, StreamErr, StreamKey, Streamer, StreamerUri};
use sea_streamer_kafka::KafkaStreamer;
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
        StreamerUri::one("localhost:9092".parse().unwrap()),
        Default::default(),
    )
    .await?;
    let producer = streamer.create_producer(output, Default::default()).await?;

    for i in 0..10 {
        let message = format!("{{\"hello\": {}}}", i);
        let _fut = producer.send(message).unwrap();
        println!("{i}");
    }

    loop {
        // we need to disconnect and end producer properly
    }

    Ok(())
}
