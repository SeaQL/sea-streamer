use anyhow::Result;
use sea_streamer_file::FileStreamer;
use sea_streamer_types::{Buffer, Consumer, Message, StreamKey, Streamer, StreamerUri};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "Stream from this file")]
    file: StreamerUri,
    #[structopt(long, help = "Stream key of input")]
    stream: StreamKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { file, stream } = Args::from_args();

    let streamer = FileStreamer::connect(file, Default::default()).await?;
    let consumer = streamer
        .create_consumer(&[stream], Default::default())
        .await?;

    loop {
        let mess = consumer.next().await?;
        let h = mess.header();
        println!(
            "[{} {}] {}",
            h.stream_key(),
            h.sequence(),
            mess.message().as_str()?
        );
    }
}
