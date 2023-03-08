use anyhow::Result;
use redis::AsyncCommands;
use sea_streamer_redis::MSG;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let client = redis::Client::open("redis://localhost/")?;
    let mut con = client.get_async_connection().await?;

    for i in 0..10 {
        let res: String = con
            .xadd("my_stream_1", "*", &[(MSG, format!("hi {i}"))])
            .await?;
        println!("{res:?}");
    }

    Ok(())
}
