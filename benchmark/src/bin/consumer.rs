use anyhow::Result;
use sea_streamer::{
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, SeaConsumer, SeaConsumerOptions,
    SeaMessage, SeaStreamReset, SeaStreamer, StreamUrl, Streamer,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(
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

    let Args { stream } = Args::from_args();

    let streamer = SeaStreamer::connect(stream.streamer(), Default::default()).await?;

    let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_stream_reset(SeaStreamReset::Earliest);

    let consumer: SeaConsumer = streamer
        .create_consumer(stream.stream_keys(), options)
        .await?;

    let mut mess: Option<SeaMessage> = None;
    for i in 0..100_000 {
        mess = Some(consumer.next().await?);
        if i % 1000 == 0 {
            let mess = mess.as_ref().unwrap();
            println!("[{}] {}", mess.timestamp(), mess.message().as_str()?);
        }
    }
    let mess = mess.as_ref().unwrap();
    println!("[{}] {}", mess.timestamp(), mess.message().as_str()?);

    Ok(())
}
