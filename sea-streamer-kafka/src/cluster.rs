use sea_streamer::StreamerUri;
use std::fmt::Write;

pub(crate) fn cluster_uri(streamer: &StreamerUri) -> String {
    let mut string = String::new();
    for (i, node) in streamer.nodes.iter().enumerate() {
        write!(
            string,
            "{comma}{node}",
            comma = if i != 0 { "," } else { "" }
        )
        .unwrap();
    }
    string
}
