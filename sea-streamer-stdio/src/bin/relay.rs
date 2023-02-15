use anyhow::Result;
use sea_streamer_stdio::StdioStreamer;
use sea_streamer_types::{Consumer, Message, Producer, StreamKey, Streamer, StreamerUri};
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

    loop {
        let mess = consumer.next().await?;
        let mut value: serde_json::Value = mess.message().deserialize_json()?;
        if let serde_json::Value::Object(object) = &mut value {
            object.insert("relay".to_owned(), serde_json::Value::Bool(true));
        }
        producer.send(serde_json::to_string(&value)?.as_str())?;
    }
}
