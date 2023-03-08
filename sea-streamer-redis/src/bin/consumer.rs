use anyhow::Result;
use redis::{streams::StreamReadOptions, AsyncCommands};
use sea_streamer_redis::StreamReadReply;
use sea_streamer_types::{Buffer, Message};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let client = redis::Client::open("redis://localhost/")?;
    let mut con = client.get_async_connection().await?;

    let opts = StreamReadOptions::default().count(100).block(0);

    let res: StreamReadReply = con
        .xread_options(&["my_stream_1", "my_stream_2"], &["0", "0"], &opts)
        .await?;

    for message in res.messages {
        println!(
            "[{timestamp} | {stream_key} | {sequence}] {payload}",
            timestamp = message.timestamp(),
            stream_key = message.stream_key(),
            sequence = message.sequence(),
            payload = message.message().as_str().unwrap(),
        );
    }

    Ok(())
}
