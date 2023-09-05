use anyhow::Result;
use sea_streamer::stdio::StdioStreamer;
use sea_streamer::{Consumer, Message, Producer, StreamKey, Streamer, StreamerUri};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "Stream key of input")]
    input: StreamKey,
    #[structopt(long, help = "Stream key of output")]
    output: StreamKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input, output } = Args::from_args();

    let streamer = StdioStreamer::connect(StreamerUri::zero(), Default::default()).await?;
    let consumer = streamer
        .create_consumer(&[input], Default::default())
        .await?;
    let producer = streamer.create_producer(output, Default::default()).await?;

    for _ in 0..100_000 {
        let mess = consumer.next().await?;
        producer.send(mess.message())?;
    }

    producer.end().await?; // flush

    Ok(())
}
