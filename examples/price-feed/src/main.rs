use anyhow::{Result, bail};
use async_tungstenite::tungstenite::Message;
use clap::Parser;
use rust_decimal::Decimal;
use sea_streamer::{
    Producer, SeaProducer, SeaStreamer, Streamer, StreamerUri, TIMESTAMP_FORMAT, Timestamp,
    export::futures::{SinkExt, StreamExt},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, help = "Streamer URI", default_value = "redis://localhost")]
    streamer: StreamerUri,
}

#[derive(Debug, Serialize, Deserialize)]
struct SpreadMessage {
    #[allow(dead_code)]
    #[serde(skip_serializing)]
    channel_id: u32,
    spread: Spread,
    channel_name: String,
    pair: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Spread {
    bid: Decimal,
    ask: Decimal,
    #[serde(with = "timestamp_serde")]
    timestamp: Timestamp,
    bid_vol: Decimal,
    ask_vol: Decimal,
}

#[tokio::main]
async fn main() -> Result<()> {
    let Args { streamer } = Args::parse();

    println!("Connecting ..");
    let (mut ws, _) = async_tungstenite::tokio::connect_async("wss://ws.kraken.com/").await?;
    println!("Connected.");

    ws.send(Message::Text(
        r#"{
        "event": "subscribe",
        "pair": [
          "GBP/USD"
        ],
        "subscription": {
          "name": "spread"
        }
    }"#
        .to_owned(),
    ))
    .await?;

    loop {
        match ws.next().await {
            Some(Ok(Message::Text(data))) => {
                println!("{data}");
                if data.contains(r#""status":"subscribed""#) {
                    println!("Subscribed.");
                    break;
                }
            }
            e => bail!("Unexpected message {e:?}"),
        }
    }

    let streamer = SeaStreamer::connect(streamer, Default::default()).await?;
    let producer: SeaProducer = streamer
        .create_producer("GBP_USD".parse()?, Default::default())
        .await?;

    loop {
        match ws.next().await {
            Some(Ok(Message::Text(data))) => {
                if data == r#"{"event":"heartbeat"}"# {
                    continue;
                }
                let spread: SpreadMessage = serde_json::from_str(&data)?;
                let message = serde_json::to_string(&spread)?;
                println!("{message}");
                producer.send(message)?;
            }
            Some(Err(e)) => bail!("Socket error: {e}"),
            None => bail!("Stream ended"),
            e => bail!("Unexpected message {e:?}"),
        }
    }
}

mod timestamp_serde {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Timestamp, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        let value: Decimal = s.parse().map_err(serde::de::Error::custom)?;
        Timestamp::from_unix_timestamp_nanos(
            (value * Decimal::from(1_000_000_000)).try_into().unwrap(),
        )
        .map_err(serde::de::Error::custom)
    }

    pub fn serialize<S>(v: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(
            &v.format(TIMESTAMP_FORMAT)
                .map_err(serde::ser::Error::custom)?,
        )
    }
}
