use super::SeekFrom;
use futures::future::{Future, Ready, ready};
use std::{
    fs::Metadata,
    io::{Error as IoError, ErrorKind},
    path::Path,
};

pub struct File;

pub struct OpenOptions;

pub trait AsyncReadExt {
    type Future: Future<Output = Result<usize, IoError>>;

    fn read(&mut self, _: &mut [u8]) -> Self::Future;
}

pub trait AsyncWriteExt {
    type Future: Future<Output = Result<(), IoError>>;

    fn write_all(&mut self, _: &[u8]) -> Self::Future;
}

pub trait AsyncSeekExt {
    type Future: Future<Output = Result<u64, IoError>>;

    fn seek(&mut self, _: SeekFrom) -> Self::Future;
}

impl File {
    pub async fn open<P: AsRef<std::path::Path>>(_: P) -> Result<Self, IoError> {
        unimplemented!("Please enable a runtime")
    }

    pub async fn metadata(&self) -> Result<Metadata, IoError> {
        unimplemented!("Please enable a runtime")
    }

    pub async fn flush(&self) -> Result<(), IoError> {
        unimplemented!("Please enable a runtime")
    }

    pub async fn sync_all(&self) -> Result<(), IoError> {
        unimplemented!("Please enable a runtime")
    }
}

impl AsyncReadExt for File {
    type Future = Ready<Result<usize, IoError>>;

    fn read(&mut self, _: &mut [u8]) -> Self::Future {
        ready(Err(IoError::new(
            ErrorKind::Other,
            "Please enable a runtime",
        )))
    }
}

impl AsyncWriteExt for File {
    type Future = Ready<Result<(), IoError>>;

    fn write_all(&mut self, _: &[u8]) -> Self::Future {
        ready(Err(IoError::new(
            ErrorKind::Other,
            "Please enable a runtime",
        )))
    }
}

impl AsyncSeekExt for File {
    type Future = Ready<Result<u64, IoError>>;

    fn seek(&mut self, _: SeekFrom) -> Self::Future {
        ready(Err(IoError::new(
            ErrorKind::Other,
            "Please enable a runtime",
        )))
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    pub fn new() -> Self {
        Self
    }

    pub fn read(&mut self, _: bool) -> &mut OpenOptions {
        self
    }

    pub fn write(&mut self, _: bool) -> &mut OpenOptions {
        self
    }

    pub fn truncate(&mut self, _: bool) -> &mut OpenOptions {
        self
    }

    pub fn append(&mut self, _: bool) -> &mut OpenOptions {
        self
    }

    pub fn create(&mut self, _: bool) -> &mut OpenOptions {
        self
    }

    pub fn create_new(&mut self, _: bool) -> &mut OpenOptions {
        self
    }

    pub async fn open(&self, _: impl AsRef<Path>) -> Result<File, IoError> {
        Err(IoError::new(ErrorKind::Other, "Please enable a runtime"))
    }
}
