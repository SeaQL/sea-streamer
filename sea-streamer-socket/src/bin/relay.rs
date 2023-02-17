use std::str::FromStr;

use anyhow::{bail, Result};
use sea_streamer_kafka::AutoOffsetReset;
use sea_streamer_socket::{SeaConsumerOptions, SeaStreamer};
use sea_streamer_types::{
    Consumer, ConsumerMode, ConsumerOptions, Message, Producer, StreamKey, Streamer, StreamerUri,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "Streamer Source Uri, i.e. try `kafka://localhost:9092`")]
    input: StreamerUri,
    #[structopt(long, help = "Streamer Sink Uri, i.e. try `stdio://`")]
    output: StreamerUri,
    #[structopt(long, help = "Stream key (aka topic) to relay")]
    stream: StreamKey,
    #[structopt(long, help = "Stream from `start` or `end`", default_value = "end")]
    offset: Offset,
}

#[derive(Debug)]
enum Offset {
    Start,
    End,
}

impl FromStr for Offset {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "start" => Ok(Self::Start),
            "end" => Ok(Self::End),
            _ => Err("unknown Offset"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args {
        input,
        output,
        stream,
        offset,
    } = Args::from_args();

    if input == output && input.protocol() != Some("stdio") {
        bail!("input == output !!!");
    }

    let source = SeaStreamer::connect(input, Default::default()).await?;
    let mut options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    options.set_kafka_consumer_options(|options| {
        options.set_auto_offset_reset(match offset {
            Offset::Start => AutoOffsetReset::Earliest,
            Offset::End => AutoOffsetReset::Latest,
        });
    });
    let consumer = source.create_consumer(&[stream.clone()], options).await?;

    let sink = SeaStreamer::connect(output, Default::default()).await?;
    let producer = sink.create_producer(stream, Default::default()).await?;

    loop {
        let mess = consumer.next().await?;
        producer.send(mess.message())?;
    }
}
