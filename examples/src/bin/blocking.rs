use anyhow::Result;
use clap::Parser;
use flume::bounded;
use std::time::Duration;

use sea_streamer::{
    Buffer, Consumer, ConsumerMode, ConsumerOptions, Message, Producer, SeaConsumer,
    SeaConsumerOptions, SeaMessage, SeaProducer, SeaStreamer, SharedMessage, StreamUrl, Streamer,
    runtime::{sleep, spawn_task},
};

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

const NUM_THREADS: usize = 4; // Every one has at least 2 cores with 2 hyperthreads these days ... right? RIGHT?

#[cfg_attr(feature = "runtime-tokio", tokio::main)]
#[cfg_attr(feature = "runtime-smol", smol_potat::main)]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { input, output } = Args::parse();

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

    // Spawn some threads
    let mut threads: Vec<_> = (0..NUM_THREADS)
        .map(|i| {
            let producer = producer.clone();
            let receiver = receiver.clone();
            // This is an OS thread, so it can use up 100% of a pseudo CPU core
            std::thread::spawn::<_, Result<()>>(move || {
                loop {
                    let message = receiver.recv()?;
                    let message = process(i, message)?;
                    producer.send(message)?; // send is non-blocking
                }
            })
        })
        .collect();

    // Handle errors if the threads exit unexpectedly
    loop {
        watch(&mut threads);
        // We can still do async IO here
        sleep(Duration::from_secs(1)).await;
    }
}

// Here we simulate a slow, blocking function
fn process(i: usize, message: SharedMessage) -> Result<String> {
    std::thread::sleep(Duration::from_secs(1));
    Ok(format!(
        "[thread {i}] {m} processed",
        m = message.message().as_str()?
    ))
}

fn watch(threads: &mut Vec<std::thread::JoinHandle<Result<()>>>) {
    for (i, thread) in threads.iter().enumerate() {
        if thread.is_finished() {
            panic!("thread {i} exited: {err:?}", err = threads.remove(i).join());
        }
    }
}
