use sea_streamer_file::FileId;
use sea_streamer_types::Timestamp;
use std::fs::OpenOptions;

pub fn temp_file(name: &str) -> Result<FileId, std::io::Error> {
    let path = format!("/tmp/{name}");
    let _file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;

    Ok(FileId::new(path))
}

pub fn millis_of(ts: &Timestamp) -> i64 {
    (ts.unix_timestamp_nanos() / 1_000_000) as i64
}
