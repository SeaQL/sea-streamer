use super::SeekFrom;

pub struct File;

pub trait AsyncReadExt {}

pub trait AsyncSeekExt {}

impl File {
    pub async fn open<P: AsRef<std::path::Path>>(_: P) -> Result<Self, std::io::Error> {
        unimplemented!("Please enable a runtime")
    }

    pub async fn read(&mut self, _: &mut [u8]) -> Result<usize, std::io::Error> {
        unimplemented!("Please enable a runtime")
    }

    pub async fn seek(&mut self, _: SeekFrom) -> Result<u64, std::io::Error> {
        unimplemented!("Please enable a runtime")
    }
}
