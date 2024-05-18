use anyhow::Result;
use clap::Parser;
use sea_streamer_file::export::flume::{unbounded, Receiver};
use sea_streamer_file::{AsyncFile, FileId};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Write to this file", default_value = "output.log")]
    file: FileId,
}

fn main() -> Result<()> {
    env_logger::init();
    log::info!("Please type something into the console and press enter:");

    let (sender, receiver) = unbounded();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(file_sink(receiver))
    });

    loop {
        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) => break, // this means stdin is closed
            Ok(_) => (),
            Err(e) => panic!("{e:?}"),
        }
        sender.send(line)?;
    }

    Ok(())
}

async fn file_sink(receiver: Receiver<String>) -> Result<()> {
    let Args { file } = Args::parse();
    let mut file = AsyncFile::new_ow(file).await?;

    while let Ok(line) = receiver.recv_async().await {
        file.write_all(line.as_bytes()).await?;
    }

    Ok(())
}
