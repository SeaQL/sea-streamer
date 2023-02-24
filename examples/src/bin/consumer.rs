use anyhow::Result;
use sea_streamer::{
    kafka::AutoOffsetReset, Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, SeaConsumer,
    SeaConsumerOptions, SeaMessage, SeaStreamer, StreamKey, Streamer,
};
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

    let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_kafka_consumer_options(|options| {
        options.set_auto_offset_reset(AutoOffsetReset::Earliest);
    });
    let consumer: SeaConsumer = streamer.create_consumer(&[stream], options).await?;

    loop {
        let mess: SeaMessage = consumer.next().await?;
        println!("[{}] {}", mess.timestamp(), mess.message().as_str()?);
    }
}
