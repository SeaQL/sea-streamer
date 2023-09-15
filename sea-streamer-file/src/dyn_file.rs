use std::{future::Future, pin::Pin};

use crate::{
    AsyncFile, ByteSource, Bytes, FileErr, FileId, FileReader, FileReaderFuture, FileSource,
    FileSourceFuture, ReadFrom,
};
use sea_streamer_types::{export::futures::FutureExt, SeqPos};

/// A runtime adapter of `FileReader` and `FileSource`,
/// also able to switch between the two mode of operations dynamically.
pub enum DynFileSource {
    FileReader(FileReader),
    FileSource(FileSource),
    /// If you encounter this, it's a programming mistake
    Dead,
}

pub enum FileSourceType {
    FileReader,
    FileSource,
}

pub enum DynReadFuture<'a> {
    FileReader(FileReaderFuture<'a>),
    FileSource(FileSourceFuture<'a>),
}

impl DynFileSource {
    pub async fn new(file_id: FileId, stype: FileSourceType) -> Result<Self, FileErr> {
        match stype {
            FileSourceType::FileReader => Ok(Self::FileReader(FileReader::new(file_id).await?)),
            FileSourceType::FileSource => Ok(Self::FileSource(
                FileSource::new(file_id, ReadFrom::Beginning).await?,
            )),
        }
    }

    pub async fn seek(&mut self, to: SeqPos) -> Result<u64, FileErr> {
        match self {
            Self::FileReader(file) => file.seek(to).await,
            Self::FileSource(file) => file.seek(to).await,
            Self::Dead => panic!("DynFileSource: Dead"),
        }
    }

    pub fn source_type(&self) -> FileSourceType {
        match self {
            Self::FileReader(_) => FileSourceType::FileReader,
            Self::FileSource(_) => FileSourceType::FileSource,
            Self::Dead => panic!("DynFileSource: Dead"),
        }
    }

    /// Switch to a different mode of operation.
    ///
    /// Warning: This future must not be canceled.
    pub async fn switch_to(self, stype: FileSourceType) -> Result<Self, FileErr> {
        match (self, stype) {
            (Self::Dead, _) => panic!("DynFileSource: Dead"),
            (Self::FileReader(file), FileSourceType::FileSource) => {
                let (file, offset, buffer) = file.end();
                Ok(Self::FileSource(FileSource::new_with(
                    file, offset, buffer,
                )?))
            }
            (Self::FileSource(mut src), FileSourceType::FileReader) => {
                let (file, _, _, buffer) = src.end().await;
                Ok(Self::FileReader(FileReader::new_with(
                    file,
                    src.offset(),
                    buffer,
                )?))
            }
            (myself, _) => Ok(myself),
        }
    }

    pub async fn end(self) -> AsyncFile {
        match self {
            Self::Dead => panic!("DynFileSource: Dead"),
            Self::FileReader(file) => {
                let (file, _, _) = file.end();
                file
            }
            Self::FileSource(mut src) => {
                let (file, _, _, _) = src.end().await;
                file
            }
        }
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        match self {
            Self::FileReader(file) => file.offset(),
            Self::FileSource(file) => file.offset(),
            Self::Dead => panic!("DynFileSource: Dead"),
        }
    }

    #[inline]
    pub fn file_size(&self) -> u64 {
        match self {
            Self::FileReader(file) => file.file_size(),
            Self::FileSource(file) => file.file_size(),
            Self::Dead => panic!("DynFileSource: Dead"),
        }
    }

    #[inline]
    pub(crate) async fn resize(&mut self) -> Result<u64, FileErr> {
        match self {
            Self::FileReader(file) => file.resize().await,
            Self::FileSource(_) => panic!("DynFileSource: FileSource cannot be resized"),
            Self::Dead => panic!("DynFileSource: Dead"),
        }
    }

    pub fn is_dead(&self) -> bool {
        matches!(self, Self::Dead)
    }
}

impl ByteSource for DynFileSource {
    type Future<'a> = DynReadFuture<'a>;

    fn request_bytes(&mut self, size: usize) -> Self::Future<'_> {
        match self {
            Self::FileReader(file) => DynReadFuture::FileReader(file.request_bytes(size)),
            Self::FileSource(file) => DynReadFuture::FileSource(file.request_bytes(size)),
            Self::Dead => panic!("DynFileSource: Dead"),
        }
    }
}

impl<'a> Future for DynReadFuture<'a> {
    type Output = Result<Bytes, FileErr>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::{Pending, Ready};

        match Pin::into_inner(self) {
            Self::FileReader(fut) => match Pin::new(fut).poll_unpin(cx) {
                Ready(res) => Ready(res),
                Pending => Pending,
            },
            Self::FileSource(fut) => match Pin::new(fut).poll_unpin(cx) {
                Ready(res) => Ready(res),
                Pending => Pending,
            },
        }
    }
}
