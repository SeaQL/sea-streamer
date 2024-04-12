use anyhow::Result;
use flume::bounded;
use std::time::Duration;
use structopt::StructOpt;

use sea_streamer::{
    runtime::{sleep, spawn_task},
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, Producer, SeaConsumer,
    SeaConsumerOptions, SeaMessage, SeaProducer, SeaStreamer, SharedMessage, StreamUrl, Streamer,
};

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(
        long,
        help = "Streamer URI with stream key(s), i.e. try `kafka://localhost:9092/my_topic`"
    )]
    input: StreamUrl,
    #[structopt(
        long,
        help = "Streamer URI with stream key, i.e. try `stdio:///my_stream`"
    )]
    output: StreamUrl,
}

#[cfg_attr(feature = "runtime-tokio", tokio::main)]
#[cfg_attr(feature = "runtime-async-std", async_std::main)]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input, output } = Args::from_args();

    // The queue
    let (sender, receiver) = bounded(1024);

    let streamer = SeaStreamer::connect(input.streamer(), Default::default()).await?;
    let options = SeaConsumerOptions::new(ConsumerMode::RealTime);
    let consumer: SeaConsumer = streamer
        .create_consumer(input.stream_keys(), options)
        .await?;

    // This will consume as quickly as possible. But when the queue is full, it will back off.
    // So the bounded queue also acts as a rate-limiter.
    spawn_task::<_, Result<()>>(async move {
        loop {
            let message: SeaMessage = consumer.next().await?;
            // If the queue is full, we'll wait
            sender.send_async(message.to_owned()).await?;
        }
    });

    let streamer = SeaStreamer::connect(output.streamer(), Default::default()).await?;
    let producer: SeaProducer = streamer
        .create_producer(output.stream_key()?, Default::default())
        .await?;

    for batch in 0..usize::MAX {
        // Take all messages currently buffered in the queue, but do not wait
        let mut messages: Vec<SharedMessage> = receiver.drain().collect();
        if messages.is_empty() {
            // Queue is empty, so we wait until there is something
            messages.push(receiver.recv_async().await?)
        }
        for message in process(batch, messages).await? {
            // Send is non-blocking so it does not slow down the loop
            producer.send(message)?;
        }
    }

    Ok(())
}

// Process the messages in batch
async fn process(batch: usize, messages: Vec<SharedMessage>) -> Result<Vec<String>> {
    // Here we simulate a slow operation
    sleep(Duration::from_secs(1)).await;
    messages
        .into_iter()
        .map(|message| {
            Ok(format!(
                "[batch {}] {} processed",
                batch,
                message.message().as_str()?
            ))
        })
        .collect()
}
