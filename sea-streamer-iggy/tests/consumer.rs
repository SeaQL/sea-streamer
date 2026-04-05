
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sea_streamer_iggy::{IggyConsumer, IggyErr, IggyMessage};
    use sea_streamer_types::export::futures::StreamExt;
    use sea_streamer_types::{Buffer, Consumer, Message, SeqPos, ShardId, StreamErr, StreamKey, StreamResult, Timestamp};

    fn make_consumer() -> (
        IggyConsumer,
        flume::Sender<StreamResult<IggyMessage, IggyErr>>,
    ) {
        let (sender, receiver) = flume::unbounded();
        let consumer = IggyConsumer::new(receiver, Arc::new(()));
        (consumer, sender)
    }

    fn make_message(sequence: u64, payload: &[u8]) -> IggyMessage {
        IggyMessage::new(
            StreamKey::new("topic-a").expect("valid stream key"),
            ShardId::new(0),
            sequence,
            Timestamp::now_utc(),
            payload.to_vec(),
        )
    }

    #[tokio::test]
    async fn unsupported_operations_return_expected_errors() {
        let (mut consumer, _sender) = make_consumer();
        let stream_key = StreamKey::new("topic-a").expect("valid stream key");
        let shard = ShardId::new(0);

        let seek_err = consumer
            .seek(Timestamp::now_utc())
            .await
            .expect_err("seek should be unsupported");
        assert!(matches!(
            seek_err,
            StreamErr::Unsupported(ref message) if message == "Iggy does not support seek by timestamp"
        ));

        let rewind_err = consumer
            .rewind(SeqPos::Beginning)
            .await
            .expect_err("rewind should be unsupported");
        assert!(matches!(
            rewind_err,
            StreamErr::Unsupported(ref message) if message == "Iggy does not support rewind by offset in this adapter"
        ));

        let assign_err = consumer
            .assign((stream_key.clone(), shard))
            .expect_err("assign should be unsupported");
        assert!(matches!(
            assign_err,
            StreamErr::Unsupported(ref message) if message == "Iggy uses stream/topic instead of shard assignment"
        ));

        let unassign_err = consumer
            .unassign((stream_key, shard))
            .expect_err("unassign should be unsupported");
        assert!(matches!(
            unassign_err,
            StreamErr::Unsupported(ref message) if message == "Iggy uses stream/topic instead of shard assignment"
        ));
    }

    #[tokio::test]
    async fn next_receives_message_from_channel() {
        let (consumer, sender) = make_consumer();
        sender
            .send(Ok(make_message(7, b"hello")))
            .expect("send should succeed");

        let message = consumer.next().await.expect("next should return a message");
        assert_eq!(message.sequence(), 7);
        assert_eq!(message.shard_id(), ShardId::new(0));
        assert_eq!(message.stream_key().name(), "topic-a");
        assert_eq!(message.message().as_bytes(), b"hello");
    }

    #[tokio::test]
    async fn next_maps_closed_channel_to_backend_error() {
        let (consumer, sender) = make_consumer();
        drop(sender);

        let err = consumer
            .next()
            .await
            .expect_err("closed channel should return backend error");
        assert!(matches!(err, StreamErr::Backend(IggyErr::ChannelRecv)));
    }

    #[tokio::test]
    async fn stream_yields_messages_then_ends_when_channel_closes() {
        let (mut consumer, sender) = make_consumer();
        sender
            .send(Ok(make_message(1, b"a")))
            .expect("first send should succeed");
        sender
            .send(Ok(make_message(2, b"b")))
            .expect("second send should succeed");
        drop(sender);

        let mut stream = consumer.stream();

        let first = stream
            .next()
            .await
            .expect("stream should yield first item")
            .expect("first item should be Ok");
        assert_eq!(first.sequence(), 1);
        assert_eq!(first.message().as_bytes(), b"a");

        let second = stream
            .next()
            .await
            .expect("stream should yield second item")
            .expect("second item should be Ok");
        assert_eq!(second.sequence(), 2);
        assert_eq!(second.message().as_bytes(), b"b");

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn stream_preserves_errors_sent_through_channel() {
        let (mut consumer, sender) = make_consumer();
        sender
            .send(Err(StreamErr::Unsupported("synthetic".to_owned())))
            .expect("send should succeed");
        drop(sender);

        let mut stream = consumer.stream();
        let item = stream.next().await.expect("stream should yield one item");
        assert!(matches!(
            item,
            Err(StreamErr::Unsupported(ref message)) if message == "synthetic"
        ));
        assert!(stream.next().await.is_none());
    }
}
