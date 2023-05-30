use std::time::Duration;

use flume::{
    bounded,
    r#async::{RecvFut, RecvStream},
    unbounded, Receiver, Sender, TryRecvError,
};
use sea_streamer_types::{
    export::futures::{Future, FutureExt, StreamExt},
    SeqPos,
};

use crate::{
    watcher::{new_watcher, FileEvent, Watcher},
    ByteBuffer, Bytes, FileErr, FileId, ReadFrom, BUFFER_SIZE,
};
use sea_streamer_runtime::{
    file::{AsyncReadExt, AsyncSeekExt, File, SeekFrom},
    spawn_task, timeout, TaskHandle,
};

pub trait ByteSource {
    type Future<'a>: Future<Output = Result<Bytes, FileErr>>
    where
        Self: 'a;

    #[allow(clippy::needless_lifetimes)]
    fn request_bytes<'a>(&'a mut self, size: usize) -> Self::Future<'a>;
}

/// `FileSource` treats files as a live stream of bytes.
/// It will read til the end, and will resume reading when the file grows.
/// It relies on `notify::RecommendedWatcher`, which is the OS's native notify mechanism.
/// The async API allows you to request how many bytes you need, and it will wait for those
/// bytes to come in a non-blocking fashion.
///
/// If the file is removed from the file system, the stream ends.
pub struct FileSource {
    #[allow(dead_code)]
    watcher: Watcher,
    receiver: Receiver<Result<Bytes, FileErr>>,
    buffer: ByteBuffer,
    handle: Option<TaskHandle<(File, Receiver<FileEvent>, FileId)>>,
    notify: Sender<FileEvent>,
}

impl FileSource {
    pub async fn new(file_id: FileId, read_from: ReadFrom) -> Result<Self, FileErr> {
        let mut file = File::open(file_id.path()).await.map_err(FileErr::IoError)?;
        let mut pos = 0;
        if matches!(read_from, ReadFrom::End) {
            pos = file
                .seek(SeekFrom::End(0))
                .await
                .map_err(FileErr::IoError)?;
        }
        // This allows the consumer to control the pace
        let (sender, receiver) = bounded(0);
        let (notify, event) = unbounded();
        let watcher = new_watcher(file_id.clone(), notify.clone())?;

        let handle = Self::spawn_task(file, pos, sender, event, file_id);

        Ok(Self {
            watcher,
            receiver,
            buffer: ByteBuffer::new(),
            handle: Some(handle),
            notify,
        })
    }

    fn spawn_task(
        mut file: File,
        #[allow(unused_variables)] mut pos: u64,
        sender: Sender<Result<Bytes, FileErr>>,
        events: Receiver<FileEvent>,
        file_id: FileId,
    ) -> TaskHandle<(File, Receiver<FileEvent>, FileId)> {
        spawn_task(async move {
            let mut wait = 0;
            let mut buffer = vec![0u8; BUFFER_SIZE];
            'outer: while !sender.is_disconnected() {
                #[cfg(feature = "runtime-async-std")]
                // Not sure why, there must be a subtle implementation difference.
                // This is needed only on async-std
                file.seek(SeekFrom::Start(pos)).await.unwrap();
                let bytes_read = match file.read(&mut buffer).await {
                    Ok(bytes_read) => bytes_read,
                    Err(e) => {
                        send_error(&sender, FileErr::IoError(e)).await;
                        break;
                    }
                };
                if bytes_read > 0 {
                    wait = 0;
                    pos += bytes_read as u64;
                    if sender
                        .send_async(Ok(match bytes_read {
                            1 => Bytes::Byte(buffer[0]),
                            4 => Bytes::Word([buffer[0], buffer[1], buffer[2], buffer[3]]),
                            _ => {
                                let mut bytes: Vec<u8> = Vec::new();
                                bytes.extend_from_slice(&buffer[0..bytes_read]);
                                Bytes::Bytes(bytes)
                            }
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                } else {
                    // drain all remaining events
                    loop {
                        match events.try_recv() {
                            Ok(FileEvent::Modify) => {}
                            Ok(FileEvent::Remove) => {
                                send_error(&sender, FileErr::FileRemoved).await;
                                break 'outer;
                            }
                            Ok(FileEvent::Error(e)) => {
                                send_error(&sender, FileErr::WatchError(e)).await;
                                break 'outer;
                            }
                            Ok(FileEvent::Rewatch) => {
                                log::debug!("FileSource: Rewatch");
                                break 'outer;
                            }
                            Err(TryRecvError::Disconnected) => {
                                break 'outer;
                            }
                            Err(TryRecvError::Empty) => break,
                        }
                    }
                    // sleep, but there is no guarantee that OS will notify us timely, or at all
                    let result = timeout(Duration::from_millis(wait), events.recv_async()).await;
                    match result {
                        Ok(event) => match event {
                            Ok(FileEvent::Modify) => {
                                // continue;
                            }
                            Ok(FileEvent::Remove) => {
                                send_error(&sender, FileErr::FileRemoved).await;
                                break 'outer;
                            }
                            Ok(FileEvent::Error(e)) => {
                                send_error(&sender, FileErr::WatchError(e)).await;
                                break 'outer;
                            }
                            Ok(FileEvent::Rewatch) => {
                                log::debug!("FileSource: Rewatch");
                                break 'outer;
                            }
                            Err(_) => {
                                break 'outer;
                            }
                        },
                        Err(_) => {
                            // timed out
                            wait = std::cmp::min(1.max(wait * 2), 1024);
                        }
                    }
                }
            }
            log::debug!("FileSource task finish ({})", file_id.path());

            async fn send_error(sender: &Sender<Result<Bytes, FileErr>>, e: FileErr) {
                if let Err(e) = sender.send_async(Err(e)).await {
                    log::error!("{}", e.into_inner().err().unwrap());
                }
            }

            (file, events, file_id)
        })
    }

    /// Stream bytes from file. If there is no bytes, it will wait until there are,
    /// like `tail -f`.
    ///
    /// If there are some bytes in the buffer, it yields immediately.
    pub async fn stream_bytes(&mut self) -> Result<Bytes, FileErr> {
        loop {
            let size = self.buffer.size();
            if size > 0 {
                return Ok(self.buffer.consume(size));
            }
            self.receive().await?;
        }
    }

    fn receive(&mut self) -> ReceiveFuture<'_> {
        ReceiveFuture {
            buffer: &mut self.buffer,
            future: self.receiver.recv_async(),
        }
    }

    /// Seek the file stream to a different position. SeqNo is regarded as byte offset.
    ///
    /// Returns the file offset after sought.
    ///
    /// Warning: This future must not be canceled.
    pub async fn seek(&mut self, to: SeqPos) -> Result<u64, FileErr> {
        // Create a fresh channel
        let (sender, receiver) = bounded(0);
        // Drops the old channel; this may stop the task
        self.receiver = receiver;
        // Notify the task in case it is sleeping
        self.notify
            .send(FileEvent::Rewatch) // unbounded, never blocks
            .expect("FileSource: task panic");
        // Wait for task exit
        let (mut file, event, path) = self
            .handle
            .take()
            .expect("This future must not be canceled")
            .await
            .expect("FileSource: task panic");
        // Seek!
        let pos = file
            .seek(match to {
                SeqPos::Beginning => SeekFrom::Start(0),
                SeqPos::End => SeekFrom::End(0),
                SeqPos::At(to) => SeekFrom::Start(to),
            })
            .await
            .map_err(FileErr::IoError)?;
        // Spawn new task
        event.drain();
        self.handle = Some(Self::spawn_task(file, pos, sender, event, path));
        // Clear the buffer
        self.buffer.clear();
        Ok(pos)
    }
}

impl ByteSource for FileSource {
    type Future<'a> = FileByteStream<'a>;

    /// Stream N bytes from file. If there is not enough bytes, it will wait until there are,
    /// like `tail -f`.
    ///
    /// If there are enough bytes in the buffer, it yields immediately.
    fn request_bytes(&mut self, size: usize) -> Self::Future<'_> {
        FileByteStream {
            size,
            buffer: &mut self.buffer,
            stream: self.receiver.stream(),
        }
    }
}

pub struct ReceiveFuture<'a> {
    buffer: &'a mut ByteBuffer,
    future: RecvFut<'a, Result<Bytes, FileErr>>,
}

pub struct FileByteStream<'a> {
    size: usize,
    buffer: &'a mut ByteBuffer,
    stream: RecvStream<'a, Result<Bytes, FileErr>>,
}

impl<'a> Future for ReceiveFuture<'a> {
    type Output = Result<(), FileErr>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};

        match self.future.poll_unpin(cx) {
            Ready(res) => Ready(match res {
                Ok(Ok(bytes)) => {
                    self.buffer.append(bytes);
                    Ok(())
                }
                Ok(Err(e)) => Err(e),
                Err(_) => {
                    // Channel closed
                    Err(FileErr::TaskDead("Source ReceiveFuture"))
                }
            }),
            Pending => Pending,
        }
    }
}

/// A hand unrolled version of the following
/// ```ignore
/// loop {
///     if self.buffer.size() >= size {
///         return Ok(self.buffer.consume(size));
///     }
///     self.receive().await?;
/// }
/// ```
impl<'a> Future for FileByteStream<'a> {
    type Output = Result<Bytes, FileErr>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};

        loop {
            if self.buffer.size() >= self.size {
                let size = self.size;
                return Ready(Ok(self.buffer.consume(size)));
            }

            match self.stream.poll_next_unpin(cx) {
                Ready(res) => match res {
                    Some(Ok(bytes)) => {
                        self.buffer.append(bytes);
                    }
                    Some(Err(e)) => {
                        return Ready(Err(e));
                    }
                    None => {
                        // Channel closed
                        return Ready(Err(FileErr::TaskDead("Source FileByteStream")));
                    }
                },
                Pending => {
                    return Pending;
                }
            }
        }
    }
}
