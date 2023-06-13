use std::time::Duration;

use flume::{bounded, r#async::RecvStream, unbounded, Receiver, Sender, TryRecvError};
use sea_streamer_types::{
    export::futures::{Future, StreamExt},
    SeqPos,
};

use crate::{
    watcher::{new_watcher, FileEvent, Watcher},
    AsyncFile, ByteBuffer, Bytes, FileErr, FileId, ReadFrom,
};
use sea_streamer_runtime::{spawn_task, timeout, TaskHandle};

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
    handle: Option<TaskHandle<(AsyncFile, Receiver<FileEvent>)>>,
    notify: Sender<FileEvent>,
    offset: u64,
    file_size: u64,
    buffer: ByteBuffer,
}

impl FileSource {
    pub async fn new(file_id: FileId, read_from: ReadFrom) -> Result<Self, FileErr> {
        let mut file = AsyncFile::new_r(file_id).await?;
        let offset = if matches!(read_from, ReadFrom::End) {
            file.seek(SeqPos::End).await?
        } else {
            0
        };
        let buffer = ByteBuffer::new();
        Self::new_with(file, offset, buffer)
    }

    pub(crate) fn new_with(
        file: AsyncFile,
        offset: u64,
        buffer: ByteBuffer,
    ) -> Result<Self, FileErr> {
        // This allows the consumer to control the pace
        let (sender, receiver) = bounded(0);
        let (notify, event) = unbounded();
        let watcher = new_watcher(file.id(), notify.clone())?;
        let file_size = file.size();

        let handle = Self::spawn_task(file, sender, event);

        Ok(Self {
            watcher,
            receiver,
            handle: Some(handle),
            notify,
            offset,
            file_size,
            buffer,
        })
    }

    /// Offset in the file that has been read up to (i.e. by the user, not by the OS).
    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Known size of the current file. This is not always up to date,
    /// the file may have grown larger but not yet known.
    #[inline]
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    fn spawn_task(
        mut file: AsyncFile,
        sender: Sender<Result<Bytes, FileErr>>,
        events: Receiver<FileEvent>,
    ) -> TaskHandle<(AsyncFile, Receiver<FileEvent>)> {
        spawn_task(async move {
            let mut wait = 0;
            'outer: while !sender.is_disconnected() {
                let bytes = match file.read().await {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        send_error(&sender, err).await;
                        break;
                    }
                };
                if !bytes.is_empty() {
                    wait = 0;
                    if sender.send_async(Ok(bytes)).await.is_err() {
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
            log::debug!("FileSource task finish ({})", file.id().path());

            async fn send_error(sender: &Sender<Result<Bytes, FileErr>>, e: FileErr) {
                if let Err(e) = sender.send_async(Err(e)).await {
                    log::error!("{}", e.into_inner().err().unwrap());
                }
            }

            (file, events)
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
                self.offset += size as u64;
                return Ok(self.buffer.consume(size));
            }
            self.receive().await?;
        }
    }

    async fn receive(&mut self) -> Result<(), FileErr> {
        match self.receiver.recv_async().await {
            Ok(Ok(bytes)) => {
                self.buffer.append(bytes);
                self.file_size =
                    std::cmp::max(self.file_size, self.offset + self.buffer.size() as u64);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                // Channel closed
                Err(FileErr::TaskDead("FileSource::receive"))
            }
        }
    }

    /// Seek the file stream to a different position.
    /// SeqNo is regarded as byte offset.
    /// Returns the file offset after sought.
    ///
    /// Warning: This future must not be canceled.
    pub async fn seek(&mut self, to: SeqPos) -> Result<u64, FileErr> {
        let (mut file, sender, event, _) = self.end().await;
        // Seek!
        self.offset = file.seek(to).await?;
        self.file_size = std::cmp::max(self.file_size, self.offset);
        // Spawn new task
        event.drain();
        self.handle = Some(Self::spawn_task(file, sender, event));
        Ok(self.offset)
    }

    /// Stop the existing task and reclaim it's resources.
    /// Also clears the buffer.
    pub(crate) async fn end(
        &mut self,
    ) -> (
        AsyncFile,
        Sender<Result<Bytes, FileErr>>,
        Receiver<FileEvent>,
        ByteBuffer,
    ) {
        // Create a fresh channel
        let (sender, receiver) = bounded(0);
        // Drops the old channel; this may stop the task
        self.receiver = receiver;
        // Notify the task in case it is sleeping
        self.notify
            .send(FileEvent::Rewatch) // unbounded, never blocks
            .expect("FileSource: task panic");
        // Wait for task exit
        let (file, event) = self
            .handle
            .take()
            .expect("This future must not be canceled")
            .await
            .expect("FileSource: task panic");
        // Clear the buffer
        let buffer = self.buffer.take();
        // Return
        (file, sender, event, buffer)
    }
}

impl ByteSource for FileSource {
    type Future<'a> = FileSourceFuture<'a>;

    /// Stream N bytes from file. If there is not enough bytes, it will wait until there are,
    /// like `tail -f`.
    ///
    /// If there are enough bytes in the buffer, it yields immediately.
    fn request_bytes(&mut self, size: usize) -> Self::Future<'_> {
        FileSourceFuture {
            size,
            offset: &mut self.offset,
            file_size: &mut self.file_size,
            buffer: &mut self.buffer,
            stream: self.receiver.stream(),
        }
    }
}

pub struct FileSourceFuture<'a> {
    size: usize,
    offset: &'a mut u64,
    file_size: &'a mut u64,
    buffer: &'a mut ByteBuffer,
    stream: RecvStream<'a, Result<Bytes, FileErr>>,
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
impl<'a> Future for FileSourceFuture<'a> {
    type Output = Result<Bytes, FileErr>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};

        loop {
            if self.buffer.size() >= self.size {
                let size = self.size;
                *self.offset += size as u64;
                return Ready(Ok(self.buffer.consume(size)));
            }

            match self.stream.poll_next_unpin(cx) {
                Ready(res) => match res {
                    Some(Ok(bytes)) => {
                        self.buffer.append(bytes);
                        *self.file_size = std::cmp::max(
                            *self.file_size,
                            *self.offset + self.buffer.size() as u64,
                        );
                    }
                    Some(Err(e)) => {
                        return Ready(Err(e));
                    }
                    None => {
                        // Channel closed
                        return Ready(Err(FileErr::TaskDead("Source FileSourceFuture")));
                    }
                },
                Pending => {
                    return Pending;
                }
            }
        }
    }
}
