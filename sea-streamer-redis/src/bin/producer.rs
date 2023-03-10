use anyhow::Result;
use sea_streamer_redis::RedisStreamer;
use sea_streamer_types::{Producer, StreamUrl, Streamer};
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
    let producer = streamer
        .create_producer(stream.stream_key()?, Default::default())
        .await?;

    for i in 0..10 {
        let message = format!("{{\"hello\": {i}}}");
        producer.send(message)?;
    }

    producer.flush().await?;

    Ok(())
}
