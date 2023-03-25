use std::fmt::Write;

use crate::KafkaErr;
use sea_streamer_types::StreamerUri;

pub(crate) fn cluster_uri(streamer: &StreamerUri) -> Result<String, KafkaErr> {
    let mut string = String::new();
    for (i, node) in streamer.nodes().iter().enumerate() {
        write!(
            string,
            "{comma}{node}",
            comma = if i != 0 { "," } else { "" },
            node = node.as_str().trim_start_matches("kafka://")
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

#[cfg(test)]
#[test]
fn test_cluster_uri() {
    let uri: StreamerUri = "kafka://localhost:9092".parse().unwrap();
    assert_eq!(cluster_uri(&uri).unwrap(), "localhost:9092");
    let uri: StreamerUri = "localhost:9092".parse().unwrap();
    assert_eq!(cluster_uri(&uri).unwrap(), "localhost:9092");
    let uri: StreamerUri = "kafka://host-a:9092,host-b:9092".parse().unwrap();
    assert_eq!(cluster_uri(&uri).unwrap(), "host-a:9092,host-b:9092");
}
