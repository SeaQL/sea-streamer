use anyhow::Result;
use sea_streamer::{
    Consumer, ConsumerMode, ConsumerOptions, Message, Sendable, StreamKey, Streamer, StreamerUri,
};
use sea_streamer_kafka::{AutoOffsetReset, KafkaConsumerOptions, KafkaStreamer};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "Input stream")]
    input: StreamKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input } = Args::from_args();

    let streamer = KafkaStreamer::connect(
        StreamerUri::one("localhost:9092".parse().unwrap()),
        Default::default(),
    )
    .await?;
    let mut options = KafkaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_offset_reset(AutoOffsetReset::Earliest);
    let consumer = streamer.create_consumer(&[input], options).await?;

    loop {
        let mess = consumer.next().await?;
        println!("{}", mess.message().as_str()?);
    }
}
