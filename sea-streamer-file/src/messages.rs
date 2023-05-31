use std::{collections::BTreeMap, path::Path};

use sea_streamer_types::{
    export::futures::{future::BoxFuture, FutureExt},
    Buffer, Message as MessageTrait, MessageHeader, OwnedMessage, SeqNo, SeqPos, ShardId,
    SharedMessage, StreamKey, Timestamp, SEA_STREAMER_INTERNAL,
};

use crate::{
    format::{Beacon, Beacons, Checksum, Header, Message, RunningChecksum},
    ByteBuffer, ByteSource, Bytes, DynFileSource, FileErr, FileId, FileSink, FileSourceType,
    StreamMode, WriteFrom,
};

pub const END_OF_STREAM: &str = "EOS";

/// A high level file reader that demux messages and beacons
pub struct MessageSource {
    source: DynFileSource,
    buffer: ByteBuffer,
    offset: u64,
    beacon_interval: u32,
    beacons: Vec<Beacon>,
}

/// A high level file writer that mux messages and beacons
pub struct MessageSink {
    sink: FileSink,
    offset: u64,
    beacon_interval: u32,
    beacons: BTreeMap<(StreamKey, ShardId), BeaconState>,
    beacon_count: u32,
    message_count: u32,
}

struct BeaconState {
    seq_no: SeqNo,
    ts: Timestamp,
    running_checksum: RunningChecksum,
}

impl MessageSource {
    /// Creates a new message source. The file must be read from beginning,
    /// because the file header is needed.
    pub async fn new(file_id: FileId, mode: StreamMode) -> Result<Self, FileErr> {
        let mut source = DynFileSource::new(
            file_id,
            match mode {
                StreamMode::Live | StreamMode::LiveReplay => FileSourceType::FileSource,
                StreamMode::Replay => FileSourceType::FileReader,
            },
        )
        .await?;
        let header = Header::read_from(&mut source).await?;
        assert!(Header::size() < header.beacon_interval as usize);
        let mut stream = Self::new_with(source, Header::size() as u64, header.beacon_interval);
        if mode == StreamMode::Live {
            stream.rewind(SeqPos::End).await?;
        }
        Ok(stream)
    }

    /// Creates a new message source with the header read and skipped.
    pub fn new_with(source: DynFileSource, offset: u64, beacon_interval: u32) -> Self {
        Self {
            source,
            buffer: ByteBuffer::new(),
            offset,
            beacon_interval,
            beacons: Vec::new(),
        }
    }

    /// Rewind the message stream to a coarse position.
    /// SeqNo is regarded as the N-th beacon.
    /// Returns the current location in terms of N-th beacon.
    ///
    /// Warning: This future must not be canceled.
    pub async fn rewind(&mut self, target: SeqPos) -> Result<u32, FileErr> {
        let pos = match target {
            SeqPos::Beginning | SeqPos::At(0) => SeqPos::At(Header::size() as u64),
            SeqPos::End => SeqPos::End,
            SeqPos::At(nth) => {
                let at = nth * self.beacon_interval as u64;
                if at < self.known_size() {
                    SeqPos::At(at)
                } else {
                    SeqPos::End
                }
            }
        };
        self.offset = self.source.seek(pos).await?;

        // Align at a beacon
        if pos == SeqPos::End {
            let max = self.known_size() - (self.known_size() % self.beacon_interval as u64);
            let pos = match target {
                SeqPos::Beginning | SeqPos::At(0) => unreachable!(),
                SeqPos::End => max,
                SeqPos::At(nth) => {
                    let at = nth * self.beacon_interval as u64;
                    if at < self.known_size() {
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
        }

        // Now we are at the first message after the last beacon,
        // we want to consume all messages up to known size
        if matches!(target, SeqPos::End) && self.offset < self.known_size() {
            let mut next = self.offset;
            let bytes = self
                .source
                .request_bytes((self.known_size() - self.offset) as usize)
                .await?;
            let mut buffer = ByteBuffer::one(bytes);
            while let Ok(message) = Message::read_from(&mut buffer).await {
                next += message.size() as u64;
            }
            self.offset = self.source.seek(SeqPos::At(next)).await?;
        }

        Ok((self.offset / self.beacon_interval as u64) as u32)
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
            self.buffer.append(bytes); // these are message bytes

            debug_assert!(!self.buffer.size() > size, "we should never over-read");
            if self.buffer.size() == size {
                return Ok(self.buffer.consume(size));
            }
        }
    }

    /// Switch the file source type.
    ///
    /// Warning: This future must not be canceled.
    pub async fn switch_to(&mut self, stype: FileSourceType) -> Result<(), FileErr> {
        let source = std::mem::replace(&mut self.source, DynFileSource::Dead);
        self.source = source.switch_to(stype).await?;
        Ok(())
    }

    /// Read the next message.
    pub async fn next(&mut self) -> Result<Message, FileErr> {
        let mess = Message::read_from(self).await?;
        Ok(mess)
    }

    pub fn beacons(&self) -> &[Beacon] {
        &self.beacons
    }

    #[inline]
    fn known_size(&self) -> u64 {
        self.source.file_size()
    }
}

impl ByteSource for MessageSource {
    type Future<'a> = BoxFuture<'a, Result<Bytes, FileErr>>; // Too complex to unroll by hand, I give up. Just box it.

    fn request_bytes(&mut self, size: usize) -> Self::Future<'_> {
        self.request_bytes(size).boxed()
    }
}

impl MessageSink {
    pub async fn new(file_id: FileId, beacon_interval: u32, limit: u64) -> Result<Self, FileErr> {
        let path: &Path = file_id.path().as_ref();
        let file_name = path.file_name().unwrap().to_str().unwrap().to_owned();
        let mut sink = FileSink::new(file_id, WriteFrom::Beginning, limit).await?;
        let header = Header {
            file_name,
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
            message_count: 0,
        })
    }

    /// This future is cancel safe. If it's canceled after polled once, the message
    /// will have been written. Otherwise it will be dropped.
    pub async fn write(&mut self, message: OwnedMessage) -> Result<Checksum, FileErr> {
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
                    .skip(self.beacon_count as usize % self.beacons.len())
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
                self.beacon_count += beacon_count as u32;
            }
        }

        let receipt = self.message_count;
        self.sink.marker(self.message_count)?;
        self.message_count += 1;
        assert_eq!(receipt, self.sink.receipt().await?);

        Ok(checksum)
    }
}

pub fn end_of_stream() -> OwnedMessage {
    let header = MessageHeader::new(
        StreamKey::new(SEA_STREAMER_INTERNAL).unwrap(),
        ShardId::new(0),
        0,
        Timestamp::now_utc(),
    );
    OwnedMessage::new(header, END_OF_STREAM.into_bytes())
}

pub fn is_end_of_stream(mess: &SharedMessage) -> bool {
    mess.header().stream_key().name() == SEA_STREAMER_INTERNAL
        && mess.message().as_bytes() == END_OF_STREAM.as_bytes()
}
