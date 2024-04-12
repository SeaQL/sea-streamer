//! This program decodes a binary SeaStreamer .ss file and output as plain text.
//!
//! Example `log` format:
//!
//! ```ignore
//! # header
//! [2023-06-05T13:55:53.001 | hello | 1 | 0] message-1
//! # beacon
//! ```
//!
//! Example `ndjson` format:
//!
//! ```ignore
//! /* header */
//! {"header":{"stream_key":"hello","shard_id":0,"sequence":1,"timestamp":"2023-06-05T13:55:53.001"},"payload":"message-1"}
//! /* beacon */
//! ```
use anyhow::Result;
use sea_streamer_file::{
    format::MessageJson, is_end_of_stream, FileErr, FileId, MessageSource, StreamMode,
};
use sea_streamer_types::{Buffer, Message, TIMESTAMP_FORMAT};
use std::str::FromStr;
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(long, help = "Decode this file")]
    file: FileId,
    #[clap(long, help = "If set, skip printing the payload")]
    header_only: bool,
    #[clap(long, help = "The output format", default_value = "log")]
    format: Format,
}

enum Format {
    Log,
    Ndjson,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let Args {
        file,
        header_only,
        format,
    } = Args::parse();

    let mut source = MessageSource::new(file, StreamMode::Replay).await?;

    // print the header
    match format {
        Format::Log => print!("# "),
        Format::Ndjson => print!("/* "),
    }
    print!("{}", serde_json::to_string(source.file_header())?);
    match format {
        Format::Log => (),
        Format::Ndjson => print!(" */"),
    }
    println!();

    let mut beacon = 0;
    loop {
        let message = match source.next().await {
            Ok(m) => Ok(m),
            Err(FileErr::NotEnoughBytes) => {
                log::warn!("The file might have been truncated.");
                break;
            }
            Err(e) => Err(e),
        }?;
        let header = message.message.header();
        match format {
            Format::Log => {
                print!(
                    "[{} | {} | {} | {}]",
                    header.timestamp().format(TIMESTAMP_FORMAT)?,
                    header.stream_key(),
                    header.sequence(),
                    header.shard_id().id(),
                );
                if !header_only {
                    if let Ok(string) = message.message.message().as_str() {
                        print!(" {string}");
                    } else {
                        print!(" <BINARY BLOB>");
                    }
                }
                println!();
            }
            Format::Ndjson => {
                let payload = message.message.message();
                println!(
                    "{}",
                    serde_json::to_string(&MessageJson {
                        header,
                        payload: if header_only {
                            None
                        } else {
                            Some(if let Ok(string) = payload.as_str() {
                                serde_json::from_str(string)
                                    .unwrap_or(serde_json::Value::String(string.to_owned()))
                            } else {
                                let bytes: Vec<_> = payload
                                    .into_bytes()
                                    .into_iter()
                                    .map(|b| serde_json::Value::Number(b.into()))
                                    .collect();
                                serde_json::Value::Array(bytes)
                            })
                        }
                    })?
                );
            }
        }

        // print the beacon
        if source.beacon().0 != beacon {
            beacon = source.beacon().0;
            let json_string = serde_json::to_string(source.beacon().1)?;

            match format {
                Format::Log => print!("# "),
                Format::Ndjson => print!("/* "),
            }
            print!("{}", json_string);
            match format {
                Format::Log => (),
                Format::Ndjson => print!(" */"),
            }
            println!();
        }

        if is_end_of_stream(&message.message) {
            log::info!("Stream ended.");
            break;
        }
    }

    Ok(())
}

impl FromStr for Format {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "log" => Ok(Self::Log),
            "ndjson" => Ok(Self::Ndjson),
            _ => Err("Invalid Format"),
        }
    }
}
