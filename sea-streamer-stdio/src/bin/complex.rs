//! This is just to demonstrate and the more complex behaviour of the streamer
//! 
//! Should later put this under a test framework that can manage subprocesses
//!
//! ```
//! cargo run --bin clock -- --interval 1s --stream-key clock | cargo run --bin complex -- --input clock --output relay
//!
//! [2022-12-06T20:39:24.641704 | relay | 0] {"relay":1,"tick":0}
//! [2022-12-06T20:39:25.644041 | relay | 1] {"relay":2,"tick":1}
//! [2022-12-06T20:39:26.646282 | relay | 2] {"relay":1,"tick":2}
//! [2022-12-06T20:39:27.646520 | relay | 3] {"relay":2,"tick":3}
//! [2022-12-06T20:39:28.649161 | relay | 4] {"relay":1,"tick":4}
//! [2022-12-06T20:39:29.650107 | relay | 5] {"relay":2,"tick":5}
//! [2022-12-06T20:39:30.650880 | relay | 6] {"relay":1,"tick":6}
//! [2022-12-06T20:39:31.653151 | relay | 7] {"relay":2,"tick":7}
//! [2022-12-06T20:39:32.654672 | relay | 8] {"relay":1,"tick":8}
//! [2022-12-06T20:39:33.657368 | relay | 9] {"relay":2,"tick":9}
//! [2022-12-06T20:39:34Z INFO  sea_streamer_stdio::consumers] stdin thread exit
//! [2022-12-06T20:39:34Z INFO  sea_streamer_stdio::consumers] stdin thread spawned
//! [2022-12-06T20:39:35.659592 | relay | 10] {"relay":0,"tick":11}
//! [2022-12-06T20:39:36.661385 | relay | 11] {"relay":0,"tick":12}
//! [2022-12-06T20:39:37.663200 | relay | 12] {"relay":0,"tick":13}
//! [2022-12-06T20:39:38.664979 | relay | 13] {"relay":0,"tick":14}
//! [2022-12-06T20:39:39.667204 | relay | 14] {"relay":0,"tick":15}
//! [2022-12-06T20:39:40.668413 | relay | 15] {"relay":0,"tick":16}
//! [2022-12-06T20:39:41.670094 | relay | 16] {"relay":0,"tick":17}
//! [2022-12-06T20:39:42.671961 | relay | 17] {"relay":0,"tick":18}
//! [2022-12-06T20:39:43.674300 | relay | 18] {"relay":0,"tick":19}
//! [2022-12-06T20:39:44.674986 | relay | 19] {"relay":0,"tick":20}
//! ```
use anyhow::Result;
use sea_streamer::{
    Consumer, ConsumerGroup, ConsumerOptions, Producer, StreamKey, Streamer, StreamerUri,
};
use sea_streamer_stdio::{StdioConsumerOptions, StdioStreamer};
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
    let mut consumer_opt = StdioConsumerOptions::new(sea_streamer::ConsumerMode::RealTime);
    consumer_opt.set_consumer_group(ConsumerGroup::new("abc".to_owned()))?;
    let producer = streamer.create_producer(output, Default::default()).await?;

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

    loop {
        let mess = consumer.next().await?;
        let mut value: serde_json::Value = mess.message().deserialize_json()?;
        if let serde_json::Value::Object(object) = &mut value {
            object.insert("relay".to_owned(), serde_json::Value::Number(0.into()));
        }
        producer.send(serde_json::to_string(&value)?.as_str())?;
    }
}
