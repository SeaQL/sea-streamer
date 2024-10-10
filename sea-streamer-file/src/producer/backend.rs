use flume::{unbounded, Receiver, Sender};
use sea_streamer_runtime::{spawn_task, AsyncMutex, TaskHandle};
use std::{collections::HashMap, num::NonZeroU32};

use super::{Request, RequestTo};
use crate::{
    format::{Checksum, Header, RunningChecksum},
    BeaconReader, BeaconState, ByteBuffer, DynFileSource, FileConnectOptions, FileErr, FileId,
    FileProducer, FileProducerOptions, FileReader, FileSink, MessageSink, MessageSource,
    StreamMode,
};
use sea_streamer_types::{
    Message, MessageHeader, OwnedMessage, SeqNo, SeqPos, ShardId, StreamKey, Timestamp,
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
struct Writers {
    writers: HashMap<FileId, Writer>,
    ending: Vec<Writer>,
}

struct Writer {
    /// 1 -> 1 channel
    sender: Sender<Request>,
    count: usize,
}

struct StreamState {
    seq_no: SeqNo,
    ts: Timestamp,
    checksum: Checksum,
}

impl Default for StreamState {
    fn default() -> Self {
        Self {
            seq_no: 0,
            ts: Timestamp::now_utc(),
            checksum: Checksum(RunningChecksum::init()),
        }
    }
}

pub(crate) async fn new_producer(
    file_id: FileId,
    options: &FileConnectOptions,
    pro_options: &FileProducerOptions,
) -> Result<FileProducer, FileErr> {
    let mut writers = WRITERS.lock().await;
    writers.add(file_id, options, pro_options).await
}

pub(crate) fn end_producer(file_id: FileId) -> Receiver<Result<(), FileErr>> {
    let (s, r) = unbounded();
    SENDER
        .0
        .send(RequestTo {
            file_id,
            data: Request::End(s),
        })
        .expect("It's a static; after all");
    r
}

impl Writers {
    fn new() -> Self {
        let _handle = spawn_task(async move {
            while let Ok(req) = SENDER.1.recv_async().await {
                let mut writers = WRITERS.lock().await;
                if let Err((file_id, err)) = writers.dispatch(req) {
                    log::error!("{err}: {file_id}");
                    writers.remove(&file_id);
                }
            }
        });

        Self {
            writers: Default::default(),
            ending: Default::default(),
        }
    }

    async fn add(
        &mut self,
        file_id: FileId,
        options: &FileConnectOptions,
        _pro_options: &FileProducerOptions,
    ) -> Result<FileProducer, FileErr> {
        if !self.writers.contains_key(&file_id) {
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
            master: &SENDER.0,
            sender: writer.sender.clone(),
        })
    }

    /// Returns the owned Writer, if count reaches 0.
    fn drop(&mut self, file_id: &FileId) -> Option<Writer> {
        if let Some(writer) = self.writers.get_mut(file_id) {
            writer.count -= 1;
            if writer.count == 0 {
                return self.writers.remove(file_id);
            }
        }
        None
    }

    fn remove(&mut self, file_id: &FileId) {
        self.writers.remove(file_id);
    }

    fn dispatch(&mut self, request: RequestTo) -> Result<(), (FileId, FileErr)> {
        if matches!(request.data, Request::Drop) {
            // if this Writer becomes orphaned, remove it
            if let Some(writer) = self.drop(&request.file_id) {
                if writer.sender.send(Request::Drop).is_err() {
                    return Err((request.file_id, FileErr::FileRemoved));
                }
                self.ending.push(writer);
            }
        } else if matches!(request.data, Request::Clone) {
            if let Some(writer) = self.writers.get_mut(&request.file_id) {
                writer.count += 1;
            }
        } else if matches!(request.data, Request::End(_)) {
            if let Some(writer) = self.writers.remove(&request.file_id) {
                if writer.sender.send(request.data).is_err() {
                    return Err((request.file_id, FileErr::FileRemoved));
                }
                // we should wait until the Writer task actually received the request
                self.ending.push(writer);
            }
        } else {
            log::error!("Please send the request to the Writer directly {request:?}");
        }
        self.ending.retain(|s| !s.sender.is_disconnected());
        Ok(())
    }
}

impl Writer {
    async fn new(file_id: FileId, options: &FileConnectOptions) -> Result<Self, FileErr> {
        let end_with_eos = options.end_with_eos();
        let file_size_limit = options.file_size_limit();
        let mut sink =
            MessageSink::append(file_id.clone(), options.beacon_interval(), file_size_limit)
                .await?;
        let (sender, receiver) = unbounded::<Request>();
        let mut streams: HashMap<(StreamKey, ShardId), StreamState> = Default::default();
        #[cfg(feature = "runtime-async-std")]
        let mut last_flush = std::time::Instant::now();

        let _handle: TaskHandle<Result<(), FileErr>> = spawn_task(async move {
            'outer: while let Ok(request) = receiver.recv_async().await {
                match request {
                    Request::Send(req) => {
                        debug_assert!(req.receipt.is_empty());
                        let key = (req.stream_key.clone(), req.shard_id);
                        let stream = if let Some(stream) = streams.get_mut(&key) {
                            // we tracked the stream state
                            stream
                        } else if sink.started_from() == Header::size() as u64 {
                            // this is a fresh new file stream
                            streams.entry(key).or_default()
                        } else {
                            // we'll need to seek backwards until we find the last message of the same stream

                            // 0. temporarily take ownership of the file
                            let file = sink.take_file().await?;
                            let mut reader = FileReader::new_with(file, 0, ByteBuffer::new())?;
                            assert_eq!(0, reader.seek(SeqPos::Beginning).await?);
                            let source = DynFileSource::FileReader(reader);
                            let mut source =
                                MessageSource::new_with(source, StreamMode::Replay).await?;
                            // 1. go backwards from stream end and look for the latest Beacon with our stream of interest
                            let max_n = source.rewind(SeqPos::End).await?;
                            let mut n = max_n;
                            while n > 0 {
                                let beacon = source.survey(NonZeroU32::new(n).unwrap()).await?;
                                for item in beacon.items {
                                    let h = &item.header;
                                    if h.stream_key() == &key.0 && h.shard_id() == &key.1 {
                                        break;
                                    }
                                }
                                n -= 1;
                            }
                            // 2. go forward from there and read all messages up to started_from, recording the latest messages
                            source.rewind(SeqPos::At(n as SeqNo)).await?;
                            while source.offset() < sink.started_from() {
                                match source.next().await {
                                    Ok(msg) => {
                                        let m = &msg.message;
                                        let entry = streams
                                            .entry((m.stream_key(), m.shard_id()))
                                            .or_default();
                                        if entry.seq_no < m.sequence() {
                                            entry.seq_no = m.sequence();
                                            entry.ts = m.timestamp();
                                            entry.checksum = Checksum(msg.checksum);
                                        }
                                    }
                                    Err(FileErr::NotEnoughBytes) => {
                                        break;
                                    }
                                    Err(e) => {
                                        req.receipt.send(Err(e)).ok();
                                        break 'outer;
                                    }
                                }
                            }
                            // 3. return ownership of the file
                            let source = source.take_source();
                            let reader = match source {
                                DynFileSource::FileReader(r) => r,
                                _ => panic!("Must be FileReader"),
                            };
                            let (mut file, _, _) = reader.end();
                            file.seek(SeqPos::At(sink.offset() as SeqNo)).await?; // restore offset
                            sink.use_file(FileSink::new(file, file_size_limit)?);
                            // now we've gone through the stream, we can safely assume the stream state
                            let entry = streams.entry(key.clone()).or_default();
                            sink.update_stream_state(
                                key,
                                BeaconState {
                                    seq_no: entry.seq_no,
                                    ts: entry.ts,
                                    running_checksum: RunningChecksum::resume(entry.checksum),
                                },
                            );
                            entry
                        };
                        // construct message
                        stream.seq_no += 1;
                        let header = MessageHeader::new(
                            req.stream_key,
                            req.shard_id,
                            stream.seq_no,
                            req.timestamp,
                        );
                        // and write!
                        let result =
                            sink.write(OwnedMessage::new(header.clone(), req.bytes.bytes()));
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
                        stream.ts = req.timestamp;
                        stream.checksum = checksum;
                        #[cfg(feature = "runtime-async-std")]
                        {
                            let now = std::time::Instant::now();
                            if now.duration_since(last_flush).as_secs() > 0 {
                                last_flush = now;
                                // Again, for async-std, we have to actively flush
                                if sink.flush().await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Request::Flush(receipt) => {
                        debug_assert!(receipt.is_empty());
                        match sink.flush().await {
                            Ok(()) => receipt.send(Ok(())),
                            Err(e) => receipt.send(Err(e)),
                        }
                        .ok();
                    }
                    Request::End(receipt) => {
                        debug_assert!(receipt.is_empty());
                        let ended = sink.end(end_with_eos).await;
                        std::mem::drop(receiver); // kill the channel
                        match ended {
                            Ok(()) => receipt.send(Ok(())),
                            Err(e) => receipt.send(Err(e)),
                        }
                        .ok();
                        break;
                    }
                    Request::Clone => {
                        panic!("Should not dispatch Request::Clone");
                    }
                    Request::Drop => {
                        sink.end(false).await.ok();
                        break;
                    }
                }
            }

            log::debug!("Writer End {}", file_id);
            Ok(())
        });

        Ok(Self { sender, count: 0 })
    }
}
