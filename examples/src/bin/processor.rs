use anyhow::Result;
use clap::Parser;
use sea_streamer::{
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, Producer, SeaConsumer,
    SeaConsumerOptions, SeaMessage, SeaProducer, SeaStreamer, StreamUrl, Streamer,
};

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key(s), i.e. try `kafka://localhost:9092/my_topic`"
    )]
    input: StreamUrl,
    #[clap(
        long,
        help = "Streamer URI with stream key, i.e. try `stdio:///my_stream`"
    )]
    output: StreamUrl,
}

#[cfg_attr(feature = "runtime-tokio", tokio::main)]
#[cfg_attr(feature = "runtime-async-std", async_std::main)]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input, output } = Args::parse();

    let streamer = SeaStreamer::connect(input.streamer(), Default::default()).await?;
    let options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    let consumer: SeaConsumer = streamer
        .create_consumer(input.stream_keys(), options)
        .await?;

    let streamer = SeaStreamer::connect(output.streamer(), Default::default()).await?;
    let producer: SeaProducer = streamer
        .create_producer(output.stream_key()?, Default::default())
        .await?;

    loop {
        let message: SeaMessage = consumer.next().await?;
        let message = process(message).await?;
        eprintln!("{message}");
        producer.send(message)?; // send is non-blocking
    }
}

// Of course this will be a complex async function
async fn process(message: SeaMessage<'_>) -> Result<String> {
    Ok(format!("{} processed", message.message().as_str()?))
}
