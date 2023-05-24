use sea_streamer_types::export::futures::{future::BoxFuture, FutureExt};

use crate::{
    format::{Beacon, Beacons},
    ByteBuffer, ByteSource, Bytes, FileErr, FileSource,
};

pub struct MessageSource {
    source: FileSource,
    buffer: ByteBuffer,
    offset: u64,
    beacon_interval: u16,
    beacons: Vec<Beacon>,
}

impl MessageSource {
    pub fn new(source: FileSource, beacon_interval: u16) -> Self {
        Self {
            source,
            buffer: ByteBuffer::new(),
            offset: 0,
            beacon_interval,
            beacons: Vec::new(),
        }
    }

    pub async fn request_bytes(&mut self, size: usize) -> Result<Bytes, FileErr> {
        loop {
            if self.offset > 0 && self.offset % self.beacon_interval as u64 == 0 {
                let beacons = Beacons::read_from(&mut self.source).await?;
                self.offset += beacons.size() as u64;
                self.beacons = beacons.items;
            }

            let chunk = std::cmp::min(
                size - self.buffer.size(), // remaining size
                self.beacon_interval as usize
                    - (self.offset % self.beacon_interval as u64) as usize, // should not read past the next beacon
            );
            let bytes = self.source.request_bytes(chunk).await?;
            self.offset += chunk as u64;
            self.buffer.append(bytes); // these are message bytes

            debug_assert!(!self.buffer.size() > size, "we should never over-read");
            if self.buffer.size() == size {
                return Ok(self.buffer.consume(size));
            }
        }
    }
}

impl ByteSource for MessageSource {
    type Future<'a> = BoxFuture<'a, Result<Bytes, FileErr>>; // Too complex to unroll by hand, I give up. Just box it.

    fn request_bytes(&mut self, size: usize) -> Self::Future<'_> {
        self.request_bytes(size).boxed()
    }
}
