use crate::{ByteBuffer, Bytes, FileErr};
use flume::{bounded, unbounded, Receiver, TryRecvError};
use notify::{event::ModifyKind, Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

const BUFFER_SIZE: usize = 1024;

// `FileStream` treats files as a live stream of bytes.
// It will read til the end, and will resume reading when the file grows.
// It relies on `notify::RecommendedWatcher`, which is the OS's native notify mechanism.
// The async API allows you to request how many bytes you need, and it will wait for those
// bytes to come in a non-blocking fashion.
pub struct FileStream {
    watcher: Option<RecommendedWatcher>,
    receiver: Receiver<Result<Bytes, FileErr>>,
    buffer: ByteBuffer,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReadFrom {
    Beginning,
    End,
}

enum FileEvent {
    Modify,
    Remove,
    Error(notify::Error),
}

impl FileStream {
    pub fn new<P: AsRef<Path>>(path: P, read_from: ReadFrom) -> Result<Self, FileErr> {
        if !path.as_ref().is_file() {
            return Err(FileErr::NotFound(path.as_ref().display().to_string()));
        }
        let mut file = File::open(&path).map_err(|e| FileErr::IoError(e))?;
        let mut can_read = true;
        if matches!(read_from, ReadFrom::End) {
            file.seek(SeekFrom::End(0)).unwrap();
            can_read = false;
        }
        let mut buffer = vec![0u8; BUFFER_SIZE];
        // This allows the consumer to control the pace
        let (sender, receiver) = bounded(0);
        let (notify, event) = unbounded();
        let mut watcher = RecommendedWatcher::new(
            move |event: Result<notify::Event, notify::Error>| {
                if let Err(e) = event {
                    notify.send(FileEvent::Error(e)).ok();
                    return;
                }
                log::trace!("{event:?}");
                match event.unwrap().kind {
                    EventKind::Modify(modify) => {
                        match modify {
                            ModifyKind::Data(_) => {
                                // only if the file grows
                                notify.send(FileEvent::Modify).ok();
                            }
                            ModifyKind::Metadata(_) => {
                                notify.send(FileEvent::Remove).ok();
                            }
                            _ => (),
                        }
                    }
                    EventKind::Any
                    | EventKind::Access(_)
                    | EventKind::Create(_)
                    | EventKind::Other => {}
                    EventKind::Remove(_) => {
                        notify.send(FileEvent::Remove).ok();
                    }
                }
            },
            Config::default(),
        )
        .map_err(|e| FileErr::WatchError(e))?;
        watcher
            .watch(path.as_ref(), RecursiveMode::Recursive)
            .map_err(|e| FileErr::WatchError(e))?;
        let path = path.as_ref().display().to_string();

        std::thread::spawn(move || {
            'outer: loop {
                if can_read {
                    // drain all remaining events
                    loop {
                        match event.try_recv() {
                            Ok(FileEvent::Modify) => {}
                            Ok(FileEvent::Remove) => {
                                if let Err(e) = sender.send(Err(FileErr::FileRemoved)) {
                                    log::error!("{}", e.into_inner().err().unwrap());
                                }
                                break 'outer;
                            }
                            Ok(FileEvent::Error(e)) => {
                                if let Err(e) = sender.send(Err(FileErr::WatchError(e))) {
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
                    let bytes_read = match file.read(&mut buffer) {
                        Ok(bytes_read) => {
                            if bytes_read == 0 {
                                can_read = false;
                            }
                            bytes_read
                        }
                        Err(e) => {
                            if let Err(e) = sender.send(Err(FileErr::IoError(e))) {
                                log::error!("{}", e.into_inner().err().unwrap());
                            }
                            break;
                        }
                    };
                    if bytes_read > 0 {
                        if sender
                            .send(Ok(match bytes_read {
                                // will block here
                                1 => Bytes::Byte(buffer[0]),
                                4 => Bytes::Word([buffer[0], buffer[1], buffer[2], buffer[3]]),
                                _ => {
                                    let mut bytes: Vec<u8> = Vec::new();
                                    bytes.extend_from_slice(&buffer[0..bytes_read]);
                                    Bytes::Bytes(bytes)
                                }
                            }))
                            .is_err()
                        {
                            break;
                        }
                    }
                } else {
                    if sender.is_disconnected() {
                        break;
                    }
                    match event.recv() {
                        Ok(FileEvent::Modify) => {
                            can_read = true;
                        }
                        Ok(FileEvent::Remove) => {
                            if let Err(e) = sender.send(Err(FileErr::FileRemoved)) {
                                log::error!("{}", e.into_inner().err().unwrap());
                            }
                            break 'outer;
                        }
                        Ok(FileEvent::Error(e)) => {
                            if let Err(e) = sender.send(Err(FileErr::WatchError(e))) {
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
            log::debug!("FileReader exit ({})", path);
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
                // The error propagates to here...
                self.watcher = None; // drop the watch, this closes the channel
                Err(e)
            }
            Err(_) => {
                // Already dead
                Err(FileErr::WatchDead)
            }
        }
    }
}
