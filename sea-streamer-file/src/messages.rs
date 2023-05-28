use std::{collections::BTreeMap, path::Path};

use sea_streamer_types::{
    export::futures::{future::BoxFuture, FutureExt},
    Message as MessageTrait, MessageHeader, OwnedMessage, SeqNo, SeqPos, ShardId, StreamKey,
    Timestamp,
};

use crate::{
    format::{Beacon, Beacons, Header, Message, RunningChecksum},
    ByteBuffer, ByteSource, Bytes, FileErr, FileSink, FileSource, ReadFrom, WriteFrom,
};

/// A high level file reader that demux messages and beacons
pub struct MessageSource {
    source: FileSource,
    buffer: ByteBuffer,
    offset: u64,
    known_size: u64,
    beacon_interval: u32,
    beacons: Vec<Beacon>,
}

/// A high level file writer that mux messages and beacons
pub struct MessageSink {
    sink: FileSink,
    offset: u64,
    beacon_interval: u32,
    beacons: BTreeMap<(StreamKey, ShardId), BeaconState>,
    beacon_count: usize,
}

struct BeaconState {
    seq_no: SeqNo,
    ts: Timestamp,
    running_checksum: RunningChecksum,
}

impl MessageSource {
    /// Creates a new message source. The file must be read from beginning,
    /// because the file header is needed.
    pub async fn new(path: &str) -> Result<Self, FileErr> {
        let mut source = FileSource::new(path, ReadFrom::Beginning).await?;
        let header = Header::read_from(&mut source).await?;
        assert!(Header::size() < header.beacon_interval as usize);
        Ok(Self::new_with(
            source,
            Header::size() as u64,
            header.beacon_interval,
        ))
    }

    /// Creates a new message source with the header read and skipped.
    pub fn new_with(source: FileSource, offset: u64, beacon_interval: u32) -> Self {
        Self {
            source,
            buffer: ByteBuffer::new(),
            offset,
            known_size: offset,
            beacon_interval,
            beacons: Vec::new(),
        }
    }

    /// Rewind the message stream to a coarse position.
    ///
    /// Note: SeqNo is regarded as the N-th beacon.
    ///
    /// Warning: This future must not be canceled.
    pub async fn rewind(&mut self, target: SeqPos) -> Result<(), FileErr> {
        let pos = match target {
            SeqPos::Beginning | SeqPos::At(0) => SeqPos::At(Header::size() as u64),
            SeqPos::End => SeqPos::End,
            SeqPos::At(nth) => {
                let at = nth * self.beacon_interval as u64;
                if at < self.known_size {
                    SeqPos::At(at)
                } else {
                    SeqPos::End
                }
            }
        };
        self.offset = self.source.seek(pos).await?;
        self.known_size = std::cmp::max(self.known_size, self.offset);

        // Align at a beacon
        if pos == SeqPos::End {
            let max = self.known_size - (self.known_size % self.beacon_interval as u64);
            let pos = match target {
                SeqPos::Beginning | SeqPos::At(0) => unreachable!(),
                SeqPos::End => max,
                SeqPos::At(nth) => {
                    let at = nth * self.beacon_interval as u64;
                    if at < self.known_size {
                        at
                    } else {
                        max
                    }
                }
            };
            self.offset = self.source.seek(SeqPos::At(pos)).await?;
        }

        self.buffer.clear();
        self.beacons.clear();

        // Read until the start of the next message
        while self.has_beacon() {
            let beacons = Beacons::read_from(&mut self.source).await?;
            let beacons_size = beacons.size();
            self.offset += beacons_size as u64;
            self.beacons = beacons.items;

            let bytes = self
                .source
                .request_bytes(std::cmp::min(
                    beacons.remaining_messages_bytes as usize,
                    self.beacon_interval as usize - beacons_size,
                ))
                .await?;
            self.offset += bytes.len() as u64;
            self.known_size = std::cmp::max(self.known_size, self.offset);
        }

        Ok(())
    }

    fn has_beacon(&self) -> bool {
        self.offset > 0 && self.offset % self.beacon_interval as u64 == 0
    }

    async fn request_bytes(&mut self, size: usize) -> Result<Bytes, FileErr> {
        loop {
            if self.has_beacon() {
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
            self.known_size = std::cmp::max(self.known_size, self.offset);
            self.buffer.append(bytes); // these are message bytes

            debug_assert!(!self.buffer.size() > size, "we should never over-read");
            if self.buffer.size() == size {
                return Ok(self.buffer.consume(size));
            }
        }
    }

    pub async fn next(&mut self) -> Result<Message, FileErr> {
        let mess = Message::read_from(self).await?;
        Ok(mess)
    }

    pub fn beacons(&self) -> &[Beacon] {
        &self.beacons
    }
}

impl ByteSource for MessageSource {
    type Future<'a> = BoxFuture<'a, Result<Bytes, FileErr>>; // Too complex to unroll by hand, I give up. Just box it.

    fn request_bytes(&mut self, size: usize) -> Self::Future<'_> {
        self.request_bytes(size).boxed()
    }
}

impl MessageSink {
    pub async fn new(path: &str, beacon_interval: u32, limit: usize) -> Result<Self, FileErr> {
        let mut sink = FileSink::new(path, WriteFrom::Beginning, limit).await?;
        let path: &Path = path.as_ref();
        let header = Header {
            file_name: path.file_name().unwrap().to_str().unwrap().to_owned(),
            created_at: Timestamp::now_utc(),
            beacon_interval,
        };
        let size = header.write_to(&mut sink)?;
        Ok(Self {
            sink,
            offset: size as u64,
            beacon_interval,
            beacons: Default::default(),
            beacon_count: 0,
        })
    }

    pub fn write(&mut self, message: OwnedMessage) -> Result<(), FileErr> {
        let key = (message.stream_key(), message.shard_id());
        let (seq_no, ts) = (message.sequence(), message.timestamp());
        let message = Message {
            message,
            checksum: 0,
        };
        let mut buffer = ByteBuffer::new();
        let (_, checksum) = message.write_to(&mut buffer)?;
        let entry = self.beacons.entry(key).or_insert(BeaconState {
            seq_no,
            ts,
            running_checksum: RunningChecksum::new(),
        });
        entry.seq_no = std::cmp::max(seq_no, entry.seq_no);
        entry.ts = std::cmp::max(ts, entry.ts);
        entry.running_checksum.update(checksum);

        while !buffer.is_empty() {
            let chunk = self.beacon_interval as usize
                - (self.offset % self.beacon_interval as u64) as usize;
            let chunk: ByteBuffer = buffer.consume(std::cmp::min(chunk, buffer.size()));
            self.offset += chunk.write_to(&mut self.sink)? as u64;

            if self.offset > 0 && self.offset % self.beacon_interval as u64 == 0 {
                let num_beacons = Beacons::num_beacons(self.beacon_interval as usize);
                let mut items = Vec::new();
                // We may not have enough space to fit in all beacons for every stream.
                // In which case, we'll round-robin among them.
                for ((key, sid), beacon) in self
                    .beacons
                    .iter()
                    .skip(self.beacon_count % self.beacons.len())
                    .chain(self.beacons.iter())
                    .take(std::cmp::min(self.beacons.len(), num_beacons))
                {
                    items.push(Beacon {
                        header: MessageHeader::new(key.to_owned(), *sid, beacon.seq_no, beacon.ts),
                        running_checksum: beacon.running_checksum.crc(),
                    });
                }
                let beacon_count = items.len();
                let beacons = Beacons {
                    remaining_messages_bytes: buffer.size() as u32,
                    items,
                };
                self.offset += beacons.write_to(&mut self.sink)? as u64;
                self.beacon_count += beacon_count;
            }
        }

        Ok(())
    }
}
