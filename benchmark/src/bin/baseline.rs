use anyhow::Result;
use sea_streamer::{runtime::sleep, StreamUrl};
use std::time::Duration;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(
        long,
        help = "Streamer URI with stream key, i.e. try `kafka://localhost:9092/my_topic`",
        env = "STREAM_URL"
    )]
    stream: StreamUrl,
}

#[cfg_attr(feature = "runtime-tokio", tokio::main)]
#[cfg_attr(feature = "runtime-async-std", async_std::main)]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { stream } = Args::from_args();
    std::hint::black_box(stream);

    for i in 0..100_000 {
        let message = format!("The this the message payload {i:0>5}: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo");
        std::hint::black_box(message);
        if i % 1000 == 0 {
            sleep(Duration::from_nanos(1)).await;
        }
    }

    Ok(())
}
