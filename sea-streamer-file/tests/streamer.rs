mod util;
use util::*;

static INIT: std::sync::Once = std::sync::Once::new();

// cargo test --test streamer --features=test,runtime-tokio -- --nocapture
// cargo test --test streamer --features=test,runtime-smol -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-smol", smol_potat::test)]
async fn streamer() -> anyhow::Result<()> {
    use sea_streamer_file::{
        AutoStreamReset, FileConnectOptions, FileConsumerOptions, FileErr, FileId, FileStreamer,
    };
    use sea_streamer_runtime::sleep;
    use sea_streamer_types::{
        Buffer, Consumer, Message, Producer, SeqNo, SharedMessage, StreamErr, StreamKey, Streamer,
        Timestamp,
    };

    enum Mode {
        Default,
        CreateOnly,
        CreateIfNotExists,
    }

    INIT.call_once(env_logger::init);
    run("streamers-1", Mode::Default).await?;
    println!("Default streamer ... ok.");
    run("streamers-2", Mode::CreateOnly).await?;
    println!("Create only ... ok.");
    run("streamers-3", Mode::CreateIfNotExists).await?;
    println!("Create if not exists ... ok.");

    async fn run(test: &'static str, mode: Mode) -> anyhow::Result<()> {
        let now = Timestamp::now_utc();
        let file_name = format!("{}-{}", test, millis_of(&now));
        let file_id = FileId::new(format!("/tmp/{file_name}"));
        println!("{file_id}");
        let stream_key = StreamKey::new("hello")?;

        let mut options = FileConnectOptions::default();
        match mode {
            Mode::Default => {
                // the file does not exist
                assert!(
                    FileStreamer::connect(file_id.to_streamer_uri()?, options.clone())
                        .await
                        .is_err()
                );
                assert_eq!(file_id, temp_file(&file_name)?);
            }
            Mode::CreateOnly => {
                options.set_create_only(true);
            }
            Mode::CreateIfNotExists => {
                options.set_create_if_not_exists(true);
            }
        }
        let pro_streamer =
            FileStreamer::connect(file_id.to_streamer_uri()?, options.clone()).await?;
        let mut producer = pro_streamer
            .create_producer(stream_key.clone(), Default::default())
            .await?;

        let con_streamer =
            FileStreamer::connect(file_id.to_streamer_uri()?, Default::default()).await?;
        let consumer = con_streamer
            .create_consumer(&[stream_key.clone()], Default::default())
            .await?;

        let check = |m: SharedMessage, i: SeqNo| {
            let h = m.header();
            assert_eq!(h.stream_key(), &stream_key);
            assert_eq!(h.sequence(), &i);
            let num: SeqNo = m.message().as_str().unwrap().parse().unwrap();
            assert_eq!(num, i);
        };

        for i in 1..10 {
            let mess = format!("{}", i);
            producer.send(mess)?;
        }
        producer.flush().await?;
        for i in 1..10 {
            check(consumer.next().await?, i);
        }

        if matches!(mode, Mode::CreateOnly) {
            // the file already exist
            assert!(
                FileStreamer::connect(file_id.to_streamer_uri()?, options)
                    .await
                    .is_err()
            );
        } else {
            // should be okay
            assert!(
                FileStreamer::connect(file_id.to_streamer_uri()?, options)
                    .await
                    .is_ok()
            );
        }

        Ok(())
    }

    Ok(())
}
