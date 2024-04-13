use anyhow::Result;
use sea_streamer_file::{FileId, MessageSink, DEFAULT_BEACON_INTERVAL, DEFAULT_FILE_SIZE_LIMIT};
use sea_streamer_redis::{AutoStreamReset, RedisConsumerOptions, RedisStreamer};
use sea_streamer_types::{
    Consumer, ConsumerMode, ConsumerOptions, Message, StreamUrl, Streamer, Timestamp,
    TIMESTAMP_FORMAT,
};
use std::time::Duration;
use clap::Parser;
use time::PrimitiveDateTime;

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key, i.e. try `redis://localhost/hello`"
    )]
    stream: StreamUrl,
    #[clap(long, help = "Output file. Overwrites if exist")]
    output: FileId,
    #[clap(long, help = "Timestamp start of range")]
    since: Option<String>,
    #[clap(long, help = "Timestamp end of range")]
    until: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args {
        stream,
        output,
        since,
        until,
    } = Args::parse();

    let since = since.map(|s| parse_timestamp(&s).unwrap());
    let until = until.map(|s| parse_timestamp(&s).unwrap());

    let streamer = RedisStreamer::connect(stream.streamer(), Default::default()).await?;
    let mut options = RedisConsumerOptions::new(ConsumerMode::RealTime);
    options.set_auto_stream_reset(AutoStreamReset::Earliest);

    let mut consumer = streamer
        .create_consumer(stream.stream_keys(), options)
        .await?;
    if let Some(since) = since {
        consumer.seek(since).await?;
    }

    let mut sink = MessageSink::new(
        output.clone(),
        DEFAULT_BEACON_INTERVAL,
        DEFAULT_FILE_SIZE_LIMIT,
    )
    .await?;

    let dur = Duration::from_secs(1);
    let mut count = 0;
    while let Ok(Ok(mess)) = tokio::time::timeout(dur, consumer.next()).await {
        if let Some(until) = &until {
            if &mess.timestamp() > until {
                break;
            }
        }
        sink.write(mess.to_owned_message())?;
        count += 1;
    }

    sink.flush().await?;
    log::info!("Written {count} messages to {output}");

    Ok(())
}

fn parse_timestamp(input: &str) -> Result<Timestamp> {
    let ts = PrimitiveDateTime::parse(input, &TIMESTAMP_FORMAT)?;
    Ok(ts.assume_utc())
}
