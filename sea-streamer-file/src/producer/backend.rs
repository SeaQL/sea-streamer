use flume::{bounded, unbounded, Receiver, Sender};
use sea_streamer_runtime::{spawn_task, AsyncMutex, TaskHandle};
use std::collections::HashMap;

use super::{Request, RequestTo};
use crate::{
    format::{Checksum, Header},
    FileConnectOptions, FileErr, FileId, FileProducer, MessageSink, MessageSource, StreamMode,
};
use sea_streamer_types::{
    export::futures::{select, FutureExt},
    ConsumerGroup, Message, MessageHeader, OwnedMessage, Receipt, SeqNo, ShardId, SharedMessage,
    StreamKey, Timestamp,
};

lazy_static::lazy_static! {
    static ref WRITERS: AsyncMutex<Writers> = AsyncMutex::new(Writers::new());
    /// N -> 1 channel
    static ref SENDER: (Sender<RequestTo>, Receiver<RequestTo>) = unbounded();
}

/// This is a process-wide singleton Writer manager. It allows multiple stream producers
/// to share the same FileSink.
///
/// It also have to keep track of the sequence ids of every (stream_key, shard_id).
///
/// Producers (N) -> SENDER (1) -> Writers (N)
struct Writers {
    writers: HashMap<FileId, Writer>,
}

struct Writer {
    /// 1 -> 1 channel
    sender: Sender<Request>,
    count: usize,
}

struct StreamState {
    seq_no: SeqNo,
    checksum: Checksum,
}

impl Writers {
    fn new() -> Self {
        let _handle = spawn_task(async move {
            while let Ok(req) = SENDER.1.recv_async().await {
                let mut writers = WRITERS.lock().await;
                writers.dispatch(req);
            }
        });

        Self {
            writers: Default::default(),
        }
    }

    async fn add(
        &mut self,
        file_id: FileId,
        options: FileConnectOptions,
    ) -> Result<FileProducer, FileErr> {
        if self.writers.get(&file_id).is_none() {
            self.writers.insert(
                file_id.clone(),
                Writer::new(file_id.clone(), options).await?,
            );
        }
        let writer = self.writers.get_mut(&file_id).unwrap();
        writer.count += 1;

        Ok(FileProducer {
            file_id,
            stream: None,
            sender: &SENDER.0,
        })
    }

    fn remove(&mut self, file_id: &FileId) -> Option<Writer> {
        if let Some(writer) = self.writers.get_mut(file_id) {
            writer.count -= 1;
            if writer.count == 0 {
                return self.writers.remove(file_id);
            }
        } else {
            log::error!("Unknown File {}", file_id);
        }
        None
    }

    fn dispatch(&mut self, request: RequestTo) {
        if matches!(request.data, Request::End) {
            if let Some(writer) = self.remove(&request.file_id) {
                if writer.sender.send(request.data).is_err() {
                    log::error!("Dead File {}", request.file_id);
                }
            }
        } else if let Some(writer) = self.writers.get(&request.file_id) {
            if writer.sender.send(request.data).is_err() {
                log::error!("Dead File {}", request.file_id);
            }
        } else {
            log::error!("Unknown File {}", request.file_id);
        }
    }
}

impl Writer {
    async fn new(file_id: FileId, options: FileConnectOptions) -> Result<Self, FileErr> {
        let mut sink = MessageSink::append(
            file_id.clone(),
            options.beacon_interval(),
            options.file_size_limit(),
        )
        .await?;
        let (sender, receiver) = unbounded::<Request>();
        let mut streams: HashMap<(StreamKey, ShardId), StreamState> = Default::default();

        let _handle: TaskHandle<Result<(), FileErr>> = spawn_task(async move {
            while let Ok(request) = receiver.recv_async().await {
                match request {
                    Request::Send(req) => {
                        let key = (req.stream_key.clone(), req.shard_id);
                        let stream = if let Some(stream) = streams.get_mut(&key) {
                            stream
                        } else {
                            if sink.started_from() == Header::size() as u64 {
                                streams.entry(key).or_insert(StreamState {
                                    seq_no: 1,
                                    checksum: Checksum(0),
                                })
                            } else {
                                // we'll need to seek backwards until we find the last message of the same stream
                                todo!()
                            }
                        };
                        let header = MessageHeader::new(
                            req.stream_key,
                            req.shard_id,
                            stream.seq_no,
                            req.timestamp,
                        );
                        let result = sink
                            .write(OwnedMessage::new(header.clone(), req.bytes.bytes()))
                            .await;
                        let checksum = match result {
                            Ok(c) => {
                                req.receipt.send(Ok(header)).ok();
                                c
                            }
                            Err(e) => {
                                req.receipt.send(Err(e)).ok();
                                break;
                            }
                        };
                        stream.seq_no += 1;
                        stream.checksum = checksum;
                    }
                    Request::End => {
                        sink.end(false).await?;
                    }
                }
            }

            log::debug!("Writer End {}", file_id);
            Ok(())
        });

        Ok(Self { sender, count: 0 })
    }
}
