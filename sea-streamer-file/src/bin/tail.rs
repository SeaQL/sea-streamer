//! A for demo `tail -f` program.
use anyhow::Result;
use sea_streamer_file::{FileId, FileSource, ReadFrom};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, help = "File path")]
    file: FileId,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args { file } = Args::from_args();
    let mut stream = FileSource::new(file, ReadFrom::End).await?;

    loop {
        let bytes = stream.stream_bytes().await?;
        print!("{}", std::str::from_utf8(&bytes.bytes())?);
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
    }
}
