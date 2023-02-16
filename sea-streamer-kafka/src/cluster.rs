use std::fmt::Write;

use crate::KafkaErr;
use sea_streamer_types::StreamerUri;

pub(crate) fn cluster_uri(streamer: &StreamerUri) -> Result<String, KafkaErr> {
    let mut string = String::new();
    for (i, node) in streamer.nodes().iter().enumerate() {
        write!(
            string,
            "{comma}{node}",
            comma = if i != 0 { "," } else { "" }
        )
        .unwrap();
    }
    if string.is_empty() {
        return Err(KafkaErr::ClientCreation(
            "StreamerUri has no nodes".to_owned(),
        ));
    }
    Ok(string)
}
