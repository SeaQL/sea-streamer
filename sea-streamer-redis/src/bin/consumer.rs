use anyhow::Result;
use sea_streamer_redis::{AutoStreamReset, RedisConsumerOptions, RedisStreamer};
use sea_streamer_types::{
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, StreamUrl, Streamer,
};
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key, i.e. try `redis://localhost/hello`",
        env = "STREAM_URL"
    )]
    stream: StreamUrl,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::parse();

    let streamer = RedisStreamer::connect(stream.streamer(), Default::default()).await?;
    let mut options = RedisConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_stream_reset(AutoStreamReset::Earliest);
    let consumer = streamer
        .create_consumer(stream.stream_keys(), options)
        .await?;

    loop {
        let mess = consumer.next().await?;
        println!("{}", mess.message().as_str()?);
    }
}
