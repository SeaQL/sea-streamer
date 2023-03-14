use anyhow::Result;
use sea_streamer_redis::RedisStreamer;
use sea_streamer_types::{Buffer, Consumer, Message, StreamUrl, Streamer};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(
        long,
        help = "Streamer URI with stream key, i.e. try `redis://localhost/hello`",
        env = "STREAM_URL"
    )]
    stream: StreamUrl,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::from_args();

    let streamer = RedisStreamer::connect(stream.streamer(), Default::default()).await?;
    let consumer = streamer
        .create_consumer(stream.stream_keys(), Default::default())
        .await?;

    loop {
        let message = consumer.next().await?;
        println!(
            "[{timestamp} | {stream_key} | {sequence}] {payload}",
            timestamp = message.timestamp(),
            stream_key = message.stream_key(),
            sequence = message.sequence(),
            payload = message.message().as_str().unwrap(),
        );
    }
}
