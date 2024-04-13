use anyhow::Result;
use clap::Parser;
use sea_streamer_kafka::{AutoOffsetReset, KafkaConsumerOptions, KafkaStreamer};
use sea_streamer_types::{
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, StreamUrl, Streamer,
};

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key(s), i.e. try `kafka://localhost:9092/hello`",
        env = "STREAM_URL"
    )]
    stream: StreamUrl,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::parse();

    let streamer = KafkaStreamer::connect(stream.streamer(), Default::default()).await?;
    let mut options = KafkaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_offset_reset(AutoOffsetReset::Earliest);
    let consumer = streamer
        .create_consumer(stream.stream_keys(), options)
        .await?;

    loop {
        let mess = consumer.next().await?;
        println!("{}", mess.message().as_str()?);
    }
}
