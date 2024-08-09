use pin_project::pin_project;
use sea_streamer_types::{export::futures::Stream, Message, StreamKey};
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    task::Poll,
};

type Keys<M> = BTreeMap<StreamKey, VecDeque<M>>;

/// Join multiple streams, but reorder messages by timestamp.
/// Since a stream can potentially infinite and the keys in the stream cannot be known priori,
/// the internal buffer can potentially grow infinite.
///
/// `align()` must be called manually to specify which streams to be aligned. Otherwise messages will be out
/// of order until the first message of each key arrives. Imagine a really stuck stream sending the first message
/// one day later, it will invalidate everything before it. But itself is the problem, not the others.
///
/// Messages within each stream key are assumed to be causal.
///
/// A typical use would be to join two streams from different sources, each with a different update frequency.
/// Messages from the fast stream will be buffered, until a message from the slow stream arrives.
///
/// ```ignore
/// fast | (1) (2) (3) (4) (5)
/// slow |         (2)         (6)
/// ```
///
/// In the example above, messages 1, 2 from fast will be buffered, until 2 from the slow stream arrives.
/// Likewise, messages 3, 4, 5 will be buffered until 6 arrives.
///
/// If two messages have the same timestamp, the order will be determined by the alphabetic order of the stream keys.
#[pin_project]
pub struct StreamJoin<S, M, E>
where
    S: Stream<Item = Result<M, E>>,
    M: Message,
    E: std::error::Error,
{
    #[pin]
    muxed: S,
    keys: Keys<M>,
    key_keys: Vec<StreamKey>,
    ended: bool,
    err: Option<E>,
}

impl<S, M, E> StreamJoin<S, M, E>
where
    S: Stream<Item = Result<M, E>>,
    M: Message,
    E: std::error::Error,
{
    /// Takes an already multiplexed stream. This can typically be achieved by `futures_concurrency::stream::Merge`.
    pub fn muxed(muxed: S) -> Self {
        Self {
            muxed,
            keys: Default::default(),
            key_keys: Default::default(),
            ended: false,
            err: None,
        }
    }

    /// Add a stream key that needs to be joined. You can call this multiple times.
    pub fn align(&mut self, stream_key: StreamKey) {
        self.keys.insert(stream_key.clone(), Default::default());
        self.key_keys.push(stream_key);
    }

    fn next(keys: &mut Keys<M>) -> Option<M> {
        let mut min_key = None;
        let mut min_ts = None;
        for (k, ms) in keys.iter() {
            if let Some(m) = ms.front() {
                let m_ts = m.timestamp();
                if min_ts.is_none() || m_ts < min_ts.unwrap() {
                    min_ts = Some(m_ts);
                    min_key = Some(k.clone());
                }
            }
        }
        if let Some(min_key) = min_key {
            Some(
                keys.get_mut(&min_key)
                    .unwrap()
                    .pop_front()
                    .expect("Checked above"),
            )
        } else {
            // all streams ended
            None
        }
    }

    fn check(keys: &Keys<M>, key_keys: &[StreamKey]) -> bool {
        // if none of the key streams are empty
        for kk in key_keys {
            if keys.get(kk).expect("Already inserted").is_empty() {
                return false;
            }
        }
        // if anyone got anything
        keys.values().any(|ms| !ms.is_empty())
    }
}

impl<S, M, E> Stream for StreamJoin<S, M, E>
where
    S: Stream<Item = Result<M, E>>,
    M: Message,
    E: std::error::Error,
{
    type Item = Result<M, E>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        while !*this.ended {
            match this.muxed.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(mes))) => {
                    let key = mes.stream_key();
                    this.keys.entry(key).or_default().push_back(mes);
                    if Self::check(&this.keys, &this.key_keys) {
                        // if we can yield
                        break;
                    }
                    // keep polling
                }
                Poll::Ready(Some(Err(err))) => {
                    *this.ended = true;
                    *this.err = Some(err);
                    break;
                }
                Poll::Ready(None) => {
                    *this.ended = true;
                    break;
                }
                Poll::Pending => {
                    // take a break
                    break;
                }
            }
        }
        if *this.ended || Self::check(&this.keys, &this.key_keys) {
            Poll::Ready(match Self::next(this.keys) {
                Some(item) => Some(Ok(item)),
                None => this.err.take().map(Err),
            })
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use sea_streamer_socket::{BackendErr, SeaMessage, SeaMessageStream};
    use sea_streamer_types::{
        export::futures::{self, TryStreamExt},
        MessageHeader, OwnedMessage, StreamErr, Timestamp,
    };

    // just to see if this compiles
    #[allow(dead_code)]
    fn wrap<'a>(
        s: SeaMessageStream<'a>,
    ) -> StreamJoin<SeaMessageStream<'a>, SeaMessage<'a>, StreamErr<BackendErr>> {
        StreamJoin::muxed(s)
    }

    fn make_seq(key: StreamKey, items: &[u64]) -> Vec<Result<OwnedMessage, BackendErr>> {
        items
            .iter()
            .copied()
            .map(|i| {
                Ok(OwnedMessage::new(
                    MessageHeader::new(
                        key.clone(),
                        Default::default(),
                        i,
                        Timestamp::from_unix_timestamp(i as i64).unwrap(),
                    ),
                    Vec::new(),
                ))
            })
            .collect()
    }

    fn compare(messages: Vec<OwnedMessage>, expected: &[(&str, u64)]) {
        assert_eq!(messages.len(), expected.len());
        for (i, m) in messages.iter().enumerate() {
            assert_eq!(m.stream_key().name(), expected[i].0);
            assert_eq!(m.sequence(), expected[i].1);
        }
    }

    #[tokio::test]
    async fn test_mux_streams_2() {
        let a = StreamKey::new("a").unwrap();
        let b = StreamKey::new("b").unwrap();
        let stream = futures::stream::iter(
            make_seq(a.clone(), &[1, 3, 5, 7, 9])
                .into_iter()
                .chain(make_seq(b.clone(), &[2, 4, 6, 8, 10]).into_iter()),
        );
        let mut join = StreamJoin::muxed(stream);
        join.align(a);
        join.align(b);
        let messages: Vec<_> = join.try_collect().await.unwrap();
        compare(
            messages,
            &[
                ("a", 1),
                ("b", 2),
                ("a", 3),
                ("b", 4),
                ("a", 5),
                ("b", 6),
                ("a", 7),
                ("b", 8),
                ("a", 9),
                ("b", 10),
            ],
        );
    }

    #[tokio::test]
    async fn test_mux_streams_2_2() {
        let a = StreamKey::new("a").unwrap();
        let b = StreamKey::new("b").unwrap();
        let stream = futures::stream::iter(
            make_seq(a.clone(), &[1, 2, 5, 8, 9])
                .into_iter()
                .chain(make_seq(b.clone(), &[3, 4, 6, 7, 10]).into_iter()),
        );
        let mut join = StreamJoin::muxed(stream);
        join.align(a);
        join.align(b);
        let messages: Vec<_> = join.try_collect().await.unwrap();
        compare(
            messages,
            &[
                ("a", 1),
                ("a", 2),
                ("b", 3),
                ("b", 4),
                ("a", 5),
                ("b", 6),
                ("b", 7),
                ("a", 8),
                ("a", 9),
                ("b", 10),
            ],
        );
    }

    #[tokio::test]
    async fn test_mux_streams_3() {
        let a = StreamKey::new("a").unwrap();
        let b = StreamKey::new("b").unwrap();
        let c = StreamKey::new("c").unwrap();
        let stream = futures::stream::iter(
            make_seq(a.clone(), &[1, 3, 5, 7, 9])
                .into_iter()
                .chain(make_seq(c.clone(), &[5]).into_iter())
                .chain(make_seq(b.clone(), &[2, 4, 6, 8, 10]).into_iter()),
        );
        let mut join = StreamJoin::muxed(stream);
        join.align(a);
        join.align(b);
        join.align(c);
        let messages: Vec<_> = join.try_collect().await.unwrap();
        compare(
            messages,
            &[
                ("a", 1),
                ("b", 2),
                ("a", 3),
                ("b", 4),
                ("a", 5),
                ("c", 5),
                ("b", 6),
                ("a", 7),
                ("b", 8),
                ("a", 9),
                ("b", 10),
            ],
        );
    }

    #[tokio::test]
    async fn test_mux_streams_4() {
        let a = StreamKey::new("a").unwrap();
        let b = StreamKey::new("b").unwrap();
        let c = StreamKey::new("c").unwrap();
        let d = StreamKey::new("d").unwrap();
        let stream = futures::stream::iter(
            make_seq(a.clone(), &[1, 3])
                .into_iter()
                .chain(make_seq(d.clone(), &[5]).into_iter())
                .chain(make_seq(b.clone(), &[2, 4]).into_iter())
                .chain(make_seq(c.clone(), &[3]).into_iter()),
        );
        let mut join = StreamJoin::muxed(stream);
        join.align(a);
        join.align(b);
        join.align(c);
        join.align(d);
        let messages: Vec<_> = join.try_collect().await.unwrap();
        compare(
            messages,
            &[("a", 1), ("b", 2), ("a", 3), ("c", 3), ("b", 4), ("d", 5)],
        );
    }
}
