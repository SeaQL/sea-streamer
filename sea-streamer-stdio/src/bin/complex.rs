//! This is just to demonstrate the more complex behaviour of the streamer.
//! Should later put this under a test framework that can manage subprocesses.
use anyhow::Result;
use sea_streamer_stdio::{StdioConsumerOptions, StdioStreamer};
use sea_streamer_types::{
    Consumer, ConsumerGroup, ConsumerMode, ConsumerOptions, Message, Producer, StreamKey, Streamer,
    StreamerUri,
};
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Stream key of input")]
    input: StreamKey,
    #[clap(long, help = "Stream key of output")]
    output: StreamKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input, output } = Args::parse();

    let streamer = StdioStreamer::connect(StreamerUri::zero(), Default::default()).await?;
    let mut consumer_opt = StdioConsumerOptions::new(ConsumerMode::LoadBalanced);
    consumer_opt.set_consumer_group(ConsumerGroup::new("abc".to_owned()))?;
    let producer = streamer
        .create_producer(output.clone(), Default::default())
        .await?;

    {
        let consumer1 = streamer
            .create_consumer(&[input.clone()], consumer_opt.clone())
            .await?;
        let consumer2 = streamer
            .create_consumer(&[input.clone()], consumer_opt.clone())
            .await?;

        for _ in 0..5 {
            let mess = consumer1.next().await?;
            let mut value: serde_json::Value = mess.message().deserialize_json()?;
            if let serde_json::Value::Object(object) = &mut value {
                object.insert("relay".to_owned(), serde_json::Value::Number(1.into()));
            }
            producer.send(serde_json::to_string(&value)?.as_str()).ok();

            let mess = consumer2.next().await?;
            let mut value: serde_json::Value = mess.message().deserialize_json()?;
            if let serde_json::Value::Object(object) = &mut value {
                object.insert("relay".to_owned(), serde_json::Value::Number(2.into()));
            }
            producer.send(serde_json::to_string(&value)?.as_str()).ok();
        }
    }

    streamer.disconnect().await?;
    let streamer = StdioStreamer::connect(StreamerUri::zero(), Default::default()).await?;
    let consumer = streamer
        .create_consumer(&[input.clone()], consumer_opt.clone())
        .await?;
    // if we don't create a new producer, `send` will return Disconnected error
    let producer = streamer.create_producer(output, Default::default()).await?;

    loop {
        let mess = consumer.next().await?;
        let mut value: serde_json::Value = mess.message().deserialize_json()?;
        if let serde_json::Value::Object(object) = &mut value {
            object.insert("relay".to_owned(), serde_json::Value::Number(0.into()));
        }
        producer.send(serde_json::to_string(&value)?.as_str())?;
    }
}
