use flume::{Sender, r#async::RecvFut, bounded, unbounded};
use std::{collections::HashMap, fmt::Debug, future::Future, sync::Mutex};

use sea_streamer_types::{
    Buffer, Message, MessageHeader, Producer as ProducerTrait, Receipt, SeqNo, ShardId,
    SharedMessage, StreamErr, StreamKey, StreamResult, Timestamp, export::futures::FutureExt,
};

use crate::{BROADCAST, PartialHeader, StdioErr, StdioResult, TIMESTAMP_FORMAT};

lazy_static::lazy_static! {
    static ref PRODUCERS: Mutex<Producers> = Default::default();
    static ref THREAD: Mutex<Option<Sender<Signal>>> = Mutex::new(None);
}

#[derive(Debug, Default)]
struct Producers {
    sequences: HashMap<StreamKey, SeqNo>,
}

enum Signal {
    SendRequest {
        message: SharedMessage,
        receipt: Sender<Receipt>,
        loopback: bool,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct StdioProducer {
    stream: Option<StreamKey>,
    request: Sender<Signal>,
    loopback: bool,
}

pub struct SendFuture {
    fut: RecvFut<'static, Receipt>,
}

const ZERO: u64 = 0;

pub(crate) fn init() {
    let mut thread = THREAD.lock().expect("Failed to lock stdout thread");
    if thread.is_none() {
        let (sender, receiver) = unbounded();
        let builder = std::thread::Builder::new().name("sea-streamer-stdio-stdout".into());
        builder
            .spawn(move || {
                log::debug!("[{pid}] stdout thread spawned", pid = std::process::id());
                // this thread locks the mutex forever
                let mut producers = PRODUCERS
                    .try_lock()
                    .expect("Should have no other thread trying to access Producers");
                while let Ok(signal) = receiver.recv() {
                    match signal {
                        Signal::SendRequest {
                            mut message,
                            receipt,
                            loopback,
                        } => {
                            // we can time the difference from send() until now()
                            message.touch(); // set timestamp to now

                            // I believe println is atomic now, so we don't have to lock stdout
                            // fn main() {
                            //     std::thread::scope(|s| {
                            //         for num in 0..100 {
                            //             s.spawn(move || {
                            //                 println!("Hello from thread number {}", num);
                            //             });
                            //         }
                            //     });
                            // }

                            // don't print empty lines
                            if message.message().size() != 0 {
                                let stream_key = message.stream_key();
                                let seq = producers.append(&stream_key);
                                println!(
                                    "[{timestamp} | {stream} | {seq}] {payload}",
                                    timestamp = message
                                        .timestamp()
                                        .format(TIMESTAMP_FORMAT)
                                        .expect("Timestamp format error"),
                                    stream = stream_key,
                                    seq = seq,
                                    payload = message
                                        .message()
                                        .as_str()
                                        .expect("Should have already checked is valid string"),
                                );
                                if loopback {
                                    let payload = message.message();
                                    super::consumer::dispatch(
                                        PartialHeader {
                                            timestamp: Some(message.timestamp()),
                                            stream_key: Some(stream_key),
                                            sequence: Some(seq),
                                            shard_id: Some(message.shard_id()),
                                        },
                                        payload.into_bytes(),
                                        0,
                                    );
                                }
                            }
                            let meta = message.take_header();
                            // we don't care if the receipt can be delivered
                            receipt.send(meta).ok();
                        }
                        Signal::Shutdown => break,
                    }
                }
                log::debug!("[{pid}] stdout thread exit", pid = std::process::id());
                {
                    let mut thread = THREAD.lock().expect("Failed to lock stdout thread");
                    thread.take(); // set to none
                }
            })
            .unwrap();
        thread.replace(sender);
    }
}

pub(crate) fn shutdown() {
    let thread = THREAD.lock().expect("Failed to lock stdout thread");
    if let Some(sender) = thread.as_ref() {
        sender
            .send(Signal::Shutdown)
            .expect("stdout thread might have been shutdown already");
    }
}

pub(crate) fn shutdown_already() -> bool {
    let thread = THREAD.lock().expect("Failed to lock stdout thread");
    thread.is_none()
}

impl Producers {
    // returns current Seq No
    fn append(&mut self, stream: &StreamKey) -> SeqNo {
        if let Some(val) = self.sequences.get_mut(stream) {
            let seq = *val;
            *val += 1;
            seq
        } else {
            self.sequences.insert(stream.to_owned(), 1);
            0
        }
    }
}

impl Future for SendFuture {
    type Output = StreamResult<MessageHeader, StdioErr>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.fut.poll_unpin(cx) {
            std::task::Poll::Ready(res) => std::task::Poll::Ready(match res {
                Ok(res) => Ok(res),
                Err(err) => Err(StreamErr::Backend(StdioErr::RecvError(err))),
            }),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Debug for SendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendFuture").finish()
    }
}

impl ProducerTrait for StdioProducer {
    type Error = StdioErr;
    type SendFuture = SendFuture;

    fn send_to<S: Buffer>(&self, stream: &StreamKey, payload: S) -> StdioResult<Self::SendFuture> {
        let payload = payload.as_str().map_err(StreamErr::Utf8Error)?.to_owned();
        // basically using this as oneshot
        let (sender, receiver) = bounded(1);
        let size = payload.len();
        self.request
            .send(Signal::SendRequest {
                message: SharedMessage::new(
                    MessageHeader::new(
                        stream.to_owned(),
                        ShardId::new(ZERO),
                        ZERO as SeqNo,
                        Timestamp::now_utc(),
                    ),
                    payload.into_bytes(),
                    0,
                    size,
                ),
                receipt: sender,
                loopback: self.loopback,
            })
            .map_err(|_| StreamErr::Backend(StdioErr::Disconnected))?;
        Ok(SendFuture {
            fut: receiver.into_recv_async(),
        })
    }

    #[inline]
    async fn end(mut self) -> StdioResult<()> {
        self.flush().await
    }

    #[inline]
    async fn flush(&mut self) -> StdioResult<()> {
        // the trick here is to send an empty message (that will be dropped) to the stdout thread
        // and wait for the receipt. By the time it returns a receipt, everything before should
        // have already been sent
        self.send_to(&StreamKey::new(BROADCAST)?, "")?.await?;
        Ok(())
    }

    fn anchor(&mut self, stream: StreamKey) -> StdioResult<()> {
        if self.stream.is_none() {
            self.stream = Some(stream);
            Ok(())
        } else {
            Err(StreamErr::AlreadyAnchored)
        }
    }

    fn anchored(&self) -> StdioResult<&StreamKey> {
        if let Some(stream) = &self.stream {
            Ok(stream)
        } else {
            Err(StreamErr::NotAnchored)
        }
    }
}

impl StdioProducer {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::new_with(false)
    }

    pub(crate) fn new_with(loopback: bool) -> Self {
        init();
        let request = {
            let thread = THREAD.lock().expect("Failed to lock stdout thread");
            thread.as_ref().expect("Should have initialized").to_owned()
        };
        Self {
            stream: None,
            request,
            loopback,
        }
    }
}
