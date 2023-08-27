use flume::{bounded, unbounded, Receiver, Sender, TryRecvError};

use crate::{
    watcher::{new_watcher, FileEvent, Watcher},
    AsyncFile, Bytes, FileErr,
};
use sea_streamer_runtime::{spawn_task, TaskHandle};

pub trait ByteSink {
    /// This should never block.
    fn write(&mut self, bytes: Bytes) -> Result<(), FileErr>;
}

const CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB

/// Buffered file writer.
///
/// If the file is removed from the file system, the stream ends.
pub struct FileSink {
    watcher: Option<Watcher>,
    sender: Sender<Request>,
    update: Receiver<Update>,
    handle: TaskHandle<AsyncFile>,
}

#[derive(Debug)]
enum Request {
    Bytes(Bytes),
    Flush(u32),
    SyncAll,
    End,
}

#[derive(Debug)]
enum Update {
    FileErr(FileErr),
    Receipt(u32),
}

#[derive(Debug)]
struct QuotaFull;

impl FileSink {
    pub fn new(mut file: AsyncFile, mut quota: u64) -> Result<Self, FileErr> {
        let (sender, pending) = unbounded();
        let (notify, update) = bounded(0);
        let (watch, event) = unbounded();
        let watcher = new_watcher(file.id(), watch)?;
        quota -= file.size();

        let handle = spawn_task(async move {
            let mut buffer = Vec::new();

            'outer: loop {
                let mut request: Result<Request, TryRecvError> = match pending.recv_async().await {
                    Ok(request) => Ok(request),
                    Err(_) => break,
                };

                let request: Result<Option<Request>, Result<(), QuotaFull>> = loop {
                    match request {
                        Ok(Request::Bytes(mut bytes)) => {
                            let mut len = bytes.len() as u64;
                            if quota < len {
                                bytes = bytes.pop(quota as usize);
                                len = quota;
                            }
                            // accumulate bytes ...
                            buffer.append(&mut bytes.bytes());

                            quota -= len;
                            if quota == 0 {
                                break Err(Err(QuotaFull));
                            }
                            if buffer.len() >= CHUNK_SIZE {
                                break (Ok(None));
                            }
                            // continue; delay write until 1) some other request 2) some error 3) queue is empty
                        }
                        Ok(request) => break Ok(Some(request)),
                        Err(TryRecvError::Disconnected) => break Err(Ok(())),
                        Err(TryRecvError::Empty) => break Ok(None),
                    }
                    request = pending.try_recv();
                };

                if !buffer.is_empty() {
                    // ... write all in one shot
                    if let Err(err) = file.write_all(&buffer).await {
                        std::mem::drop(pending); // trigger error
                        send_error(&notify, err).await;
                        break 'outer;
                    }
                    buffer.truncate(0);
                }

                if let Err(Ok(())) = request {
                    break 'outer;
                } else if let Err(Err(QuotaFull)) = request {
                    std::mem::drop(pending); // trigger error
                    send_error(&notify, FileErr::FileLimitExceeded).await;
                    break 'outer;
                }

                match request.unwrap() {
                    Some(Request::Flush(marker)) => {
                        if let Err(err) = file.flush().await {
                            std::mem::drop(pending); // trigger error
                            send_error(&notify, err).await;
                            break 'outer;
                        }
                        if notify.send_async(Update::Receipt(marker)).await.is_err() {
                            break 'outer;
                        }
                    }
                    request @ Some(Request::SyncAll | Request::End) => {
                        if let Err(err) = file.sync_all().await {
                            std::mem::drop(pending); // trigger error
                            send_error(&notify, err).await;
                            break 'outer;
                        }
                        match request {
                            Some(Request::SyncAll) => {
                                if notify.send_async(Update::Receipt(u32::MAX)).await.is_err() {
                                    break 'outer;
                                }
                            }
                            Some(Request::End) => {
                                break 'outer;
                            }
                            _ => unreachable!(),
                        }
                    }
                    Some(_) => {
                        unreachable!();
                    }
                    None => (),
                }

                loop {
                    match event.try_recv() {
                        Ok(FileEvent::Modify) => {}
                        Ok(FileEvent::Remove) => {
                            std::mem::drop(pending); // trigger error
                            send_error(&notify, FileErr::FileRemoved).await;
                            break 'outer;
                        }
                        Ok(FileEvent::Error(e)) => {
                            std::mem::drop(pending); // trigger error
                            send_error(&notify, FileErr::WatchError(e)).await;
                            break 'outer;
                        }
                        Err(TryRecvError::Disconnected) => {
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

            log::debug!("FileSink task finish ({})", file.id().path());
            file
        });

        async fn send_error(notify: &Sender<Update>, e: FileErr) {
            if let Err(e) = notify.send_async(Update::FileErr(e)).await {
                log::error!("{:?}", e.into_inner());
            }
        }

        Ok(Self {
            watcher: Some(watcher),
            sender,
            update,
            handle,
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

    pub async fn flush(&mut self, marker: u32) -> Result<(), FileErr> {
        if self.sender.send(Request::Flush(marker)).is_err() {
            self.return_err()
        } else {
            match self.update.recv_async().await {
                Ok(Update::Receipt(receipt)) => {
                    assert_eq!(receipt, marker);
                    Ok(())
                }
                Ok(Update::FileErr(err)) => Err(err),
                Err(_) => Err(FileErr::TaskDead("FileSink::flush")),
            }
        }
    }

    pub async fn sync_all(&mut self) -> Result<(), FileErr> {
        if self.sender.send(Request::SyncAll).is_err() {
            self.return_err()
        } else {
            loop {
                match self.update.recv_async().await {
                    Ok(Update::Receipt(u32::MAX)) => return Ok(()),
                    Ok(Update::Receipt(_)) => (),
                    Ok(Update::FileErr(err)) => return Err(err),
                    Err(_) => return Err(FileErr::TaskDead("FileSink::sync_all")),
                }
            }
        }
    }

    pub async fn end(mut self) -> Result<AsyncFile, FileErr> {
        if self.sender.send(Request::End).is_err() {
            Err(self.return_err().err().unwrap())
        } else {
            self.handle
                .await
                .map_err(|_| FileErr::TaskDead("FileSink::end"))
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
