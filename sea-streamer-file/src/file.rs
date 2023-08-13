use std::{fmt::Display, os::unix::prelude::MetadataExt, str::FromStr, sync::Arc};

use crate::{ByteBuffer, ByteSource, Bytes, FileErr};
use sea_streamer_runtime::file::{
    AsyncReadExt, AsyncSeekExt, AsyncWriteExt, File, OpenOptions, SeekFrom,
};
use sea_streamer_types::{
    export::futures::{future::BoxFuture, FutureExt},
    SeqPos, StreamUrlErr, StreamerUri,
};

pub(crate) const BUFFER_SIZE: usize = 1024;

/// A simple buffered and bounded file reader.
/// The implementation is much simpler than `FileSource`.
///
/// `FileReader` treats file as a fixed depot of bytes.
/// Attempt to read beyond the end will result in a `NotEnoughBytes` error.
pub struct FileReader {
    file: AsyncFile,
    /// This is the user's read offset, not the same as file's read pos
    offset: u64,
    buffer: ByteBuffer,
}

/// A minimal wrapper over async runtime's File.
pub struct AsyncFile {
    id: FileId,
    file: File,
    size: u64,
    pos: u64,
    buf: Vec<u8>,
}

pub type FileReaderFuture<'a> = BoxFuture<'a, Result<Bytes, FileErr>>;

/// Basically a file path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId {
    path: Arc<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReadFrom {
    Beginning,
    End,
}

impl FileReader {
    pub async fn new(file_id: FileId) -> Result<Self, FileErr> {
        let file = AsyncFile::new_r(file_id).await?;
        Self::new_with(file, 0, ByteBuffer::new())
    }

    pub(crate) fn new_with(
        file: AsyncFile,
        offset: u64,
        buffer: ByteBuffer,
    ) -> Result<Self, FileErr> {
        Ok(Self {
            file,
            offset,
            buffer,
        })
    }

    /// Seek the file stream to a different position.
    /// SeqNo is regarded as byte offset.
    /// Returns the file offset after sought.
    pub async fn seek(&mut self, to: SeqPos) -> Result<u64, FileErr> {
        self.offset = self.file.seek(to).await?;
        self.buffer.clear();
        Ok(self.offset)
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    #[inline]
    pub fn file_size(&self) -> u64 {
        self.file.size()
    }

    pub(crate) fn end(self) -> (AsyncFile, u64, ByteBuffer) {
        (self.file, self.offset, self.buffer)
    }

    #[inline]
    pub async fn resize(&mut self) -> Result<u64, FileErr> {
        self.file.resize().await
    }
}

impl ByteSource for FileReader {
    type Future<'a> = FileReaderFuture<'a>;

    /// Read N bytes from file. If there is not enough bytes, it will return `NotEnoughBytes` error.
    ///
    /// If there are enough bytes in the buffer, it yields immediately.
    fn request_bytes(&mut self, size: usize) -> Self::Future<'_> {
        async move {
            if self.offset + size as u64 > self.file.size() {
                return Err(FileErr::NotEnoughBytes);
            }
            loop {
                if self.buffer.size() >= size {
                    self.offset += size as u64;
                    return Ok(self.buffer.consume(size));
                }
                let bytes = self.file.read().await?;
                if bytes.is_empty() {
                    return Err(FileErr::NotEnoughBytes);
                }
                self.buffer.append(bytes);
            }
        }
        .boxed() // sadly, there is no way to name `ReadFuture`
    }
}

impl AsyncFile {
    /// Open a file for Read
    pub async fn new_r(id: FileId) -> Result<Self, FileErr> {
        log::debug!("AsyncFile Open ({}) Read", id.path());
        let file = File::open(id.path()).await.map_err(FileErr::IoError)?;
        Self::new_with(id, file).await
    }

    /// Creates a new file for Read/Write.
    /// If the file already exsits, read from the beginning.
    /// Seek to an appropriate position to append to this file.
    pub async fn new_rw(id: FileId) -> Result<Self, FileErr> {
        log::debug!("AsyncFile Open ({}) Read/Write", id.path());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        let file = options.open(id.path()).await.map_err(FileErr::IoError)?;
        Self::new_with(id, file).await
    }

    /// Creates a new file for Overwrite. If the file already exists, truncate it.
    pub async fn new_ow(id: FileId) -> Result<Self, FileErr> {
        log::debug!("AsyncFile Open ({}) Overwrite", id.path());
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        let file = options.open(id.path()).await.map_err(FileErr::IoError)?;
        Self::new_with(id, file).await
    }

    async fn new_with(id: FileId, file: File) -> Result<Self, FileErr> {
        let size = file_size_of(&file).await?;
        let pos = 0;
        let buf = vec![0u8; BUFFER_SIZE];
        Ok(Self {
            id,
            file,
            size,
            pos,
            buf,
        })
    }

    /// Read up to `BUFFER_SIZE` amount of bytes.
    pub async fn read(&mut self) -> Result<Bytes, FileErr> {
        #[cfg(feature = "runtime-async-std")]
        if self.pos >= self.size {
            // Not sure why, there must be a subtle implementation difference.
            // This is needed only on async-std, when the file grows.
            self.file
                .seek(SeekFrom::Start(self.pos))
                .await
                .map_err(FileErr::IoError)?;
        }
        let bytes_read = self
            .file
            .read(&mut self.buf)
            .await
            .map_err(FileErr::IoError)?;
        let bytes = match bytes_read {
            0 => Bytes::Empty,
            1 => Bytes::Byte(self.buf[0]),
            4 => Bytes::Word([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]),
            _ => {
                let bytes = self.buf[0..bytes_read].to_vec();
                Bytes::Bytes(bytes)
            }
        };
        self.pos += bytes_read as u64;
        self.size = std::cmp::max(self.size, self.pos);
        Ok(bytes)
    }

    #[inline]
    pub async fn write_all(&mut self, bytes: Bytes) -> Result<(), FileErr> {
        self.file
            .write_all(&bytes.bytes())
            .await
            .map_err(FileErr::IoError)
    }

    #[inline]
    pub async fn flush(&mut self) -> Result<(), FileErr> {
        self.file.flush().await.map_err(FileErr::IoError)
    }

    #[inline]
    pub async fn sync_all(&mut self) -> Result<(), FileErr> {
        self.file.sync_all().await.map_err(FileErr::IoError)
    }

    /// Seek the file stream to a different position.
    /// SeqNo is regarded as byte offset.
    /// Returns the file position after sought.
    pub async fn seek(&mut self, to: SeqPos) -> Result<u64, FileErr> {
        self.pos = self
            .file
            .seek(match to {
                SeqPos::Beginning => SeekFrom::Start(0),
                SeqPos::End => SeekFrom::End(0),
                SeqPos::At(to) => SeekFrom::Start(to),
            })
            .await
            .map_err(FileErr::IoError)?;
        self.size = std::cmp::max(self.size, self.pos);
        Ok(self.pos)
    }

    /// Get the `FileId`.
    #[inline]
    pub fn id(&self) -> FileId {
        self.id.clone()
    }

    /// Get the file's size. This updates only when the file is read or sought.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub async fn resize(&mut self) -> Result<u64, FileErr> {
        self.size = file_size_of(&self.file).await?;
        Ok(self.size)
    }
}

impl Drop for AsyncFile {
    fn drop(&mut self) {
        log::debug!("AsyncFile Close ({})", self.id.path());
    }
}

impl FileId {
    pub fn new<T: Into<String>>(path: T) -> Self {
        Self {
            path: Arc::new(path.into()),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn to_streamer_uri(&self) -> Result<StreamerUri, StreamUrlErr> {
        format!("file://{}", self.path()).parse()
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl FromStr for FileId {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s.to_owned()))
    }
}

async fn file_size_of(file: &File) -> Result<u64, FileErr> {
    Ok(file.metadata().await.map_err(FileErr::IoError)?.size())
}
