use crate::{
    watcher::{new_watcher, FileEvent, Watcher},
    ByteBuffer, Bytes, FileErr,
};
use flume::{bounded, unbounded, Receiver, TryRecvError};
use sea_streamer_runtime::{
    file::{AsyncReadExt, AsyncSeekExt, File, SeekFrom},
    spawn_task,
};

pub const BUFFER_SIZE: usize = 1024;

/// `FileSource` treats files as a live stream of bytes.
/// It will read til the end, and will resume reading when the file grows.
/// It relies on `notify::RecommendedWatcher`, which is the OS's native notify mechanism.
/// The async API allows you to request how many bytes you need, and it will wait for those
/// bytes to come in a non-blocking fashion.
///
/// If the file is removed from the file system, the stream ends.
pub struct FileSource {
    watcher: Option<Watcher>,
    receiver: Receiver<Result<Bytes, FileErr>>,
    buffer: ByteBuffer,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReadFrom {
    Beginning,
    End,
}

impl FileSource {
    pub async fn new(path: &str, read_from: ReadFrom) -> Result<Self, FileErr> {
        let mut file = File::open(path).await.map_err(FileErr::IoError)?;
        let mut can_read = true;
        if matches!(read_from, ReadFrom::End) {
            file.seek(SeekFrom::End(0))
                .await
                .map_err(FileErr::IoError)?;
            can_read = false;
        }
        // This allows the consumer to control the pace
        let (sender, receiver) = bounded(0);
        let (notify, event) = unbounded();
        let watcher = new_watcher(path, notify)?;
        let path = path.to_string();

        let _handle = spawn_task(async move {
            #[allow(unused_variables)]
            let mut pos: usize = 0;
            let mut buffer = vec![0u8; BUFFER_SIZE];
            'outer: loop {
                if can_read {
                    // drain all remaining events
                    loop {
                        match event.try_recv() {
                            Ok(FileEvent::Modify) => {}
                            Ok(FileEvent::Remove) => {
                                if let Err(e) = sender.send_async(Err(FileErr::FileRemoved)).await {
                                    log::error!("{}", e.into_inner().err().unwrap());
                                }
                                break 'outer;
                            }
                            Ok(FileEvent::Error(e)) => {
                                if let Err(e) = sender.send_async(Err(FileErr::WatchError(e))).await
                                {
                                    log::error!("{}", e.into_inner().err().unwrap());
                                }
                                break 'outer;
                            }
                            Err(TryRecvError::Disconnected) => {
                                break 'outer;
                            }
                            Err(TryRecvError::Empty) => break,
                        }
                    }
                    #[cfg(feature = "runtime-async-std")]
                    // Not sure why, there must be a subtle implementation difference.
                    // This is needed only on async-std
                    file.seek(SeekFrom::Start(pos as u64)).await.unwrap();
                    let bytes_read = match file.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            if bytes_read == 0 {
                                can_read = false;
                            }
                            bytes_read
                        }
                        Err(e) => {
                            if let Err(e) = sender.send_async(Err(FileErr::IoError(e))).await {
                                log::error!("{}", e.into_inner().err().unwrap());
                            }
                            break;
                        }
                    };
                    if bytes_read > 0 {
                        pos += bytes_read;
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
                    }
                } else {
                    if sender.is_disconnected() {
                        break;
                    }
                    match event.recv_async().await {
                        Ok(FileEvent::Modify) => {
                            can_read = true;
                        }
                        Ok(FileEvent::Remove) => {
                            if let Err(e) = sender.send_async(Err(FileErr::FileRemoved)).await {
                                log::error!("{}", e.into_inner().err().unwrap());
                            }
                            break 'outer;
                        }
                        Ok(FileEvent::Error(e)) => {
                            if let Err(e) = sender.send_async(Err(FileErr::WatchError(e))).await {
                                log::error!("{}", e.into_inner().err().unwrap());
                            }
                            break 'outer;
                        }
                        Err(_) => {
                            break 'outer;
                        }
                    }
                }
            }
            log::debug!("FileSource task finish ({})", path);
            // sender drops, then channel closes
        });

        Ok(Self {
            watcher: Some(watcher),
            receiver,
            buffer: ByteBuffer::new(),
        })
    }

    /// Stream N bytes from file. If there is not enough bytes, it will wait until there are,
    /// like `tail -f`.
    ///
    /// If there are enough bytes in the buffer, it yields immediately.
    pub async fn request_bytes(&mut self, size: usize) -> Result<Bytes, FileErr> {
        loop {
            if self.buffer.size() >= size {
                return Ok(self.buffer.consume(size));
            }
            self.receive().await?;
        }
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

    async fn receive(&mut self) -> Result<(), FileErr> {
        let res = self.receiver.recv_async().await;
        match res {
            Ok(Ok(bytes)) => {
                self.buffer.append(bytes);
                Ok(())
            }
            Ok(Err(e)) => {
                self.watcher = None; // drop the watch, this closes the channel
                Err(e)
            }
            Err(_) => {
                // Channel closed
                Err(FileErr::WatchDead)
            }
        }
    }
}
