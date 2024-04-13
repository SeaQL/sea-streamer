use anyhow::Result;
use clap::Parser;
use sea_streamer::{
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, SeaConsumer, SeaConsumerOptions,
    SeaMessage, SeaStreamReset, SeaStreamer, StreamUrl, Streamer,
};

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key(s), i.e. try `kafka://localhost:9092/my_topic`",
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

    let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_stream_reset(SeaStreamReset::Earliest);

    let consumer: SeaConsumer = streamer
        .create_consumer(stream.stream_keys(), options)
        .await?;

    loop {
        let mess: SeaMessage = consumer.next().await?;
        println!("[{}] {}", mess.timestamp(), mess.message().as_str()?);
    }
}
