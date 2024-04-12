use anyhow::Result;
use sea_streamer::{
    kafka::AutoOffsetReset,
    redis::{AutoCommit, AutoStreamReset},
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, Producer, SeaConsumer,
    SeaConsumerOptions, SeaMessage, SeaProducer, SeaStreamer, SeaStreamerBackend, StreamUrl,
    Streamer,
};
use std::time::Duration;
use clap::Parser;

const TRANSACTION: bool = true;

#[derive(Debug, Parser)]
struct Args {
    #[clap(
        long,
        help = "Streamer URI with stream key(s), i.e. try `kafka://localhost:9092/my_topic`"
    )]
    input: StreamUrl,
    #[clap(
        long,
        help = "Streamer URI with stream key, i.e. try `stdio:///my_stream`"
    )]
    output: StreamUrl,
}

#[cfg_attr(feature = "runtime-tokio", tokio::main)]
#[cfg_attr(feature = "runtime-async-std", async_std::main)]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input, output } = Args::parse();

    let streamer = SeaStreamer::connect(input.streamer(), Default::default()).await?;
    let mut options = SeaConsumerOptions::new(ConsumerMode::Resumable);
    options.set_kafka_consumer_options(|options| {
        options.set_auto_offset_reset(AutoOffsetReset::Earliest);
        options.set_enable_auto_commit(true);
        options.set_auto_commit_interval(Duration::from_secs(1));
        options.set_enable_auto_offset_store(false);
    });
    options.set_redis_consumer_options(|options| {
        options.set_auto_stream_reset(AutoStreamReset::Earliest);
        options.set_auto_commit(if TRANSACTION {
            AutoCommit::Disabled
        } else {
            AutoCommit::Rolling
        });
        // demo only; choose a larger number in your processor
        options.set_auto_commit_interval(Duration::from_secs(0));
        // demo only; choose a larger number in your processor
        options.set_batch_size(1);
    });
    let mut consumer: SeaConsumer = streamer
        .create_consumer(input.stream_keys(), options)
        .await?;

    let streamer = SeaStreamer::connect(output.streamer(), Default::default()).await?;
    let producer: SeaProducer = streamer
        .create_producer(output.stream_key()?, Default::default())
        .await?;

    loop {
        let message: SeaMessage = consumer.next().await?;
        let identifier = message.identifier();
        // wait for the delivery receipt
        producer.send(process(message).await?)?.await?;
        if let Some(consumer) = consumer.get_kafka() {
            if TRANSACTION {
                // wait until committed
                consumer.commit_with(&identifier).await?;
            } else {
                // don't wait, so it may or may not have committed
                consumer.store_offset_with(&identifier)?;
            }
        }
        if let Some(consumer) = consumer.get_redis() {
            consumer.ack_with(&identifier)?;
            if TRANSACTION {
                // wait until committed
                consumer.commit()?.await?;
            }
        }
    }
}

// Of course this will be a complex async function
async fn process(message: SeaMessage<'_>) -> Result<String> {
    Ok(format!("{} processed", message.message().as_str()?))
}
