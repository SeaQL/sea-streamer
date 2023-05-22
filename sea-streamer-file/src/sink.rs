use flume::{bounded, unbounded, Receiver, Sender, TryRecvError};
use std::sync::Mutex;

use crate::{
    watcher::{new_watcher, FileEvent, Watcher},
    Bytes, FileErr,
};
use sea_streamer_runtime::{
    file::{AsyncWriteExt, OpenOptions},
    spawn_task,
};

/// Buffered file writer.
///
/// If the file is removed from the file system, the stream ends.
pub struct FileSink {
    watcher: Mutex<Option<Watcher>>,
    writer: Sender<Bytes>,
    error: Receiver<FileErr>,
}

pub enum WriteFrom {
    /// Truncate the file
    Beginning,
    /// Append to the file
    End,
}

pub const DEFAULT_FILE_SIZE_LIMIT: usize = 16 * 1024 * 1024 * 1024; // 16GB

impl FileSink {
    pub async fn new(path: &str, write_from: WriteFrom, mut limit: usize) -> Result<Self, FileErr> {
        let mut options = OpenOptions::new();
        options.write(true).create(true);
        match write_from {
            WriteFrom::Beginning => options.truncate(true),
            WriteFrom::End => options.append(true),
        };
        let mut file = options.open(path).await.map_err(|e| FileErr::IoError(e))?;
        let (writer, pending) = unbounded::<Bytes>();
        let (notify, error) = bounded(0);
        let (watch, event) = unbounded();
        let watcher = new_watcher(path, watch)?;
        let path = path.to_string();

        _ = spawn_task(async move {
            'outer: while let Ok(mut bytes) = pending.recv_async().await {
                let mut len = bytes.len();
                if limit < len {
                    bytes = bytes.pop(limit);
                    len = limit;
                }
                if let Err(e) = file.write_all(&bytes.bytes()).await {
                    std::mem::drop(pending); // trigger error
                    if let Err(e) = notify.send_async(FileErr::IoError(e)).await {
                        log::error!("{}", e.into_inner());
                    }
                    break;
                }
                limit -= len;
                if limit == 0 {
                    std::mem::drop(pending); // trigger error
                    if let Err(e) = notify.send_async(FileErr::FileLimitExceeded).await {
                        log::error!("{}", e.into_inner());
                    }
                    break;
                }
                loop {
                    match event.try_recv() {
                        Ok(FileEvent::Modify) => {}
                        Ok(FileEvent::Remove) => {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify.send_async(FileErr::FileRemoved).await {
                                log::error!("{}", e.into_inner());
                            }
                            break 'outer;
                        }
                        Ok(FileEvent::Error(e)) => {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify.send_async(FileErr::WatchError(e)).await {
                                log::error!("{}", e.into_inner());
                            }
                            break 'outer;
                        }
                        Err(TryRecvError::Disconnected) => {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify.send_async(FileErr::WatchDead).await {
                                log::error!("{}", e.into_inner());
                            }
                            break 'outer;
                        }
                        Err(TryRecvError::Empty) => break,
                    }
                }
            }
            log::debug!("FileSink task finish ({})", path);
        });

        Ok(Self {
            watcher: Mutex::new(Some(watcher)),
            writer,
            error,
        })
    }

    /// This method never blocks
    pub fn write(&self, bytes: Bytes) -> Result<(), FileErr> {
        // never blocks
        if self.writer.send(bytes).is_err() {
            if let Ok(mut watcher) = self.watcher.try_lock() {
                // kill the watcher so we don't leak
                watcher.take();
            }
            Err(self
                .error
                .try_recv()
                .expect("The task should always wait until the error has been sent"))
        } else {
            Ok(())
        }
    }
}
