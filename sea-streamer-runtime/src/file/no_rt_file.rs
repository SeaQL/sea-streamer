use super::SeekFrom;
use futures::future::{ready, Future, Ready};
use std::io::{Error as IoError, ErrorKind};

pub struct File;

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
