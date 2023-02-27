use anyhow::Result;
use sea_streamer::{
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, Producer, SeaConsumer,
    SeaConsumerOptions, SeaMessage, SeaProducer, SeaStreamer, StreamUrl, Streamer,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(
        long,
        help = "Streamer URI with stream key(s), i.e. try `kafka://localhost:9092/my_topic`",
        env = "STREAM_URL"
    )]
    input: StreamUrl,
    #[structopt(
        long,
        help = "Streamer URI with stream key, i.e. try `stdio:///my_stream`",
        env = "STREAM_URL"
    )]
    output: StreamUrl,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input, output } = Args::from_args();

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
        println!("{message}");
        producer.send(message)?; // send is non-blocking
    }
}

// Of course this will be a complex async function
async fn process<'a>(message: SeaMessage<'a>) -> Result<String> {
    Ok(format!("{} processed", message.message().as_str()?))
}
