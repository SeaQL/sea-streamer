use flume::{bounded, unbounded, Receiver, Sender, TryRecvError};

use crate::{
    watcher::{new_watcher, FileEvent, Watcher},
    Bytes, FileErr,
};
use sea_streamer_runtime::{
    file::{AsyncWriteExt, OpenOptions},
    spawn_task,
};

pub trait ByteSink {
    /// This should never block.
    fn write(&mut self, bytes: Bytes) -> Result<(), FileErr>;
}

/// Buffered file writer.
///
/// If the file is removed from the file system, the stream ends.
pub struct FileSink {
    watcher: Option<Watcher>,
    sender: Sender<Request>,
    update: Receiver<Update>,
}

#[derive(Debug)]
enum Request {
    Bytes(Bytes),
    Marker(u32),
}

#[derive(Debug)]
enum Update {
    FileErr(FileErr),
    Receipt(u32),
}

pub enum WriteFrom {
    /// Truncate the file
    Beginning,
    /// Append to the file
    End,
}

impl FileSink {
    pub async fn new(path: &str, write_from: WriteFrom, mut limit: usize) -> Result<Self, FileErr> {
        let mut options = OpenOptions::new();
        options.write(true).create(true);
        match write_from {
            WriteFrom::Beginning => options.truncate(true),
            WriteFrom::End => options.append(true),
        };
        let mut file = options.open(path).await.map_err(FileErr::IoError)?;
        let (sender, pending) = unbounded();
        let (notify, update) = bounded(0);
        let (watch, event) = unbounded();
        let watcher = new_watcher(path, watch)?;
        let path = path.to_string();

        let _handle = spawn_task(async move {
            'outer: while let Ok(request) = pending.recv_async().await {
                match request {
                    Request::Bytes(mut bytes) => {
                        let mut len = bytes.len();
                        if limit < len {
                            bytes = bytes.pop(limit);
                            len = limit;
                        }

                        if let Err(e) = file.write_all(&bytes.bytes()).await {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify
                                .send_async(Update::FileErr(FileErr::IoError(e)))
                                .await
                            {
                                log::error!("{:?}", e.into_inner());
                            }
                            break;
                        }

                        #[cfg(feature = "runtime-async-std")]
                        if let Err(e) = file.flush().await {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify
                                .send_async(Update::FileErr(FileErr::IoError(e)))
                                .await
                            {
                                log::error!("{:?}", e.into_inner());
                            }
                            break;
                        }

                        limit -= len;
                        if limit == 0 {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify
                                .send_async(Update::FileErr(FileErr::FileLimitExceeded))
                                .await
                            {
                                log::error!("{:?}", e.into_inner());
                            }
                            break;
                        }
                    }
                    Request::Marker(marker) => {
                        if notify.send_async(Update::Receipt(marker)).await.is_err() {
                            break;
                        }
                    }
                }

                loop {
                    match event.try_recv() {
                        Ok(FileEvent::Modify) => {}
                        Ok(FileEvent::Remove) => {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify
                                .send_async(Update::FileErr(FileErr::FileRemoved))
                                .await
                            {
                                log::error!("{:?}", e.into_inner());
                            }
                            break 'outer;
                        }
                        Ok(FileEvent::Error(e)) => {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) = notify
                                .send_async(Update::FileErr(FileErr::WatchError(e)))
                                .await
                            {
                                log::error!("{:?}", e.into_inner());
                            }
                            break 'outer;
                        }
                        Err(TryRecvError::Disconnected) => {
                            std::mem::drop(pending); // trigger error
                            if let Err(e) =
                                notify.send_async(Update::FileErr(FileErr::WatchDead)).await
                            {
                                log::error!("{:?}", e.into_inner());
                            }
                            break 'outer;
                        }
                        Ok(FileEvent::Rewatch) => {
                            log::warn!("Why are we receiving this?");
                            break 'outer;
                        }
                        Err(TryRecvError::Empty) => break,
                    }
                }
            }
            log::debug!("FileSink task finish ({})", path);
        });

        Ok(Self {
            watcher: Some(watcher),
            sender,
            update,
        })
    }

    fn return_err(&mut self) -> Result<(), FileErr> {
        if self.watcher.is_some() {
            // kill the watcher so we don't leak
            self.watcher.take();
        }

        Err(loop {
            match self.update.try_recv() {
                Ok(Update::FileErr(err)) => break err,
                Ok(_) => (),
                Err(err) => {
                    panic!("The task should always wait until the error has been sent: {err}")
                }
            }
        })
    }

    pub async fn receipt(&mut self) -> Result<u32, FileErr> {
        match self.update.recv_async().await {
            Ok(Update::Receipt(receipt)) => Ok(receipt),
            Ok(Update::FileErr(err)) => Err(err),
            Err(_) => Err(FileErr::TaskDead),
        }
    }

    pub fn marker(&mut self, marker: u32) -> Result<(), FileErr> {
        if self.sender.send(Request::Marker(marker)).is_err() {
            self.return_err()
        } else {
            Ok(())
        }
    }
}

impl ByteSink for FileSink {
    /// This method never blocks
    fn write(&mut self, bytes: Bytes) -> Result<(), FileErr> {
        if self.sender.send(Request::Bytes(bytes)).is_err() {
            self.return_err()
        } else {
            Ok(())
        }
    }
}
