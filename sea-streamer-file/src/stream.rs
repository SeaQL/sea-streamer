use crate::{ByteBuffer, Bytes, FileErr};
use flume::{unbounded, Receiver, Sender};
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

const BUFFER_SIZE: usize = 1024;

// `FileStream` treats files as a live stream of bytes.
// It always read til the end, and will resume reading when the file grows.
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

enum WrappedSender<T: Send, E: Send> {
    Alive(Sender<Result<T, E>>),
    Dead,
}

impl FileStream {
    pub fn new<P: AsRef<Path>>(path: P, read_from: ReadFrom) -> Result<Self, FileErr> {
        if !path.as_ref().is_file() {
            return Err(FileErr::NotFound);
        }
        let mut file = File::open(&path).map_err(|e| FileErr::IoError(e))?;
        if matches!(read_from, ReadFrom::End) {
            file.seek(SeekFrom::End(0)).unwrap();
        }
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let (sender, receiver) = unbounded();
        let mut sender = WrappedSender::Alive(sender);
        let mut watcher = RecommendedWatcher::new(
            move |event: Result<notify::Event, notify::Error>| {
                if let Err(e) = event {
                    if let Err(e) = sender.send(Err(FileErr::WatchError(e))) {
                        log::error!("{e}");
                    }
                    return;
                }
                match event.unwrap().kind {
                    EventKind::Modify(_) => {
                        let mut bytes: Vec<u8> = Vec::new();
                        loop {
                            let bytes_read = file.read(&mut buffer).unwrap();
                            // read until EOF
                            if bytes_read == 0 {
                                break;
                            }
                            bytes.extend_from_slice(&buffer[0..bytes_read]);
                        }
                        if let Err(e) = sender.send(Ok(match bytes.len() {
                            1 => Bytes::Byte(bytes[0]),
                            4 => Bytes::Word([bytes[0], bytes[1], bytes[2], bytes[3]]),
                            _ => Bytes::Bytes(bytes),
                        })) {
                            log::error!("{e}");
                        }
                    }
                    EventKind::Any
                    | EventKind::Access(_)
                    | EventKind::Create(_)
                    | EventKind::Other => {}
                    EventKind::Remove(_) => {
                        if let Err(e) = sender.send(Err(FileErr::FileRemoved)) {
                            log::error!("{e}");
                        }
                    }
                }
            },
            Config::default(),
        )
        .map_err(|e| FileErr::WatchError(e))?;
        watcher
            .watch(path.as_ref(), RecursiveMode::Recursive)
            .map_err(|e| FileErr::WatchError(e))?;

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

impl<T: Send, E: Send> WrappedSender<T, E> {
    fn send(&mut self, item: Result<T, E>) -> Result<(), &str> {
        match self {
            Self::Alive(sender) => {
                let is_err = item.is_err();
                if sender.send(item).is_err() {
                    *self = Self::Dead;
                    return Err("WrappedSender: send failed");
                }
                if is_err {
                    // The error originates here...
                    *self = Self::Dead;
                }
                Ok(())
            }
            Self::Dead => Err("WrappedSender: dead"),
        }
    }
}
