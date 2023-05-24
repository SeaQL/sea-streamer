use crate::{FileErr, FileSink};
use std::{
    cmp::Ordering,
    collections::VecDeque,
    future::{ready, Future, Ready},
};

pub trait ByteSource {
    type Future<'a>: Future<Output = Result<Bytes, FileErr>>
    where
        Self: 'a;

    fn request_bytes<'a>(&'a mut self, size: usize) -> Self::Future<'a>;
}

#[derive(Debug, Default, Clone)]
pub struct ByteBuffer {
    buf: VecDeque<Bytes>,
}

#[derive(Clone)]
pub enum Bytes {
    Empty,
    Byte(u8),
    Word([u8; 4]),
    Bytes(Vec<u8>),
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "Empty"),
            Self::Byte(b) => write!(f, "Byte({b})"),
            Self::Word(w) => write!(f, "Word({w:?})"),
            Self::Bytes(b) => write!(f, "Bytes(len = {})", b.len()),
        }
    }
}

impl ByteBuffer {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn append(&mut self, bytes: Bytes) {
        self.buf.push_back(bytes);
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for bytes in self.buf.iter() {
            size += bytes.len();
        }
        size
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }

    pub fn consume(&mut self, size: usize) -> Bytes {
        let mut buffer = Bytes::Empty;
        let mut remaining = size;
        loop {
            if let Some(bytes) = self.buf.front() {
                match bytes.len().cmp(&remaining) {
                    Ordering::Less | Ordering::Equal => {
                        let bytes = self.buf.pop_front().unwrap();
                        remaining -= bytes.len();
                        buffer.append(bytes);
                    }
                    Ordering::Greater => {
                        buffer.append(self.buf.front_mut().unwrap().pop(remaining));
                        break;
                    }
                }
            } else {
                panic!(
                    "Not enough bytes: consuming {}, only got {}",
                    size,
                    size - remaining
                );
            }
            if remaining == 0 {
                break;
            }
        }
        buffer
    }
}

impl Bytes {
    pub fn len(&self) -> usize {
        match self {
            Bytes::Empty => 0,
            Bytes::Byte(_) => 1,
            Bytes::Word(_) => 4,
            Bytes::Bytes(bytes) => bytes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Bytes::Empty => true,
            Bytes::Byte(_) => false,
            Bytes::Word(_) => false,
            Bytes::Bytes(bytes) => bytes.is_empty(),
        }
    }

    pub fn is_byte(&self) -> bool {
        match self {
            Bytes::Empty => false,
            Bytes::Byte(_) => true,
            Bytes::Word(_) => false,
            Bytes::Bytes(bytes) => bytes.len() == 1,
        }
    }

    pub fn pop(&mut self, size: usize) -> Self {
        let len = self.len();
        match len.cmp(&size) {
            Ordering::Less => panic!("Not enough bytes: popping {}, only got {}", size, len),
            Ordering::Equal => self.take(),
            Ordering::Greater => {
                let bytes = self.take();
                match bytes {
                    Bytes::Empty => unreachable!(),
                    Bytes::Byte(_) => Self::Empty,
                    Bytes::Word(bytes) => match size {
                        0 => Self::Empty,
                        1 => {
                            *self = Self::Bytes(bytes[1..].to_vec());
                            Self::Byte(bytes[0])
                        }
                        2 | 3 => {
                            *self = Self::Bytes(bytes[size..].to_vec());
                            Self::Bytes(bytes[0..size].to_vec())
                        }
                        _ => unreachable!(),
                    },
                    Bytes::Bytes(mut ret) => {
                        let bytes = ret.split_off(size);
                        *self = Self::Bytes(bytes);
                        Self::Bytes(ret)
                    }
                }
            }
        }
    }

    pub fn bytes(self) -> Vec<u8> {
        match self {
            Bytes::Empty => vec![],
            Bytes::Byte(b) => vec![b],
            Bytes::Word([a, b, c, d]) => vec![a, b, c, d],
            Bytes::Bytes(bytes) => bytes,
        }
    }

    /// Get exactly a byte; otherwise None
    pub fn byte(&self) -> Option<u8> {
        match self {
            Bytes::Empty => None,
            Bytes::Byte(b) => Some(*b),
            Bytes::Word(_) => None,
            Bytes::Bytes(bytes) => {
                if bytes.len() == 1 {
                    Some(bytes[0])
                } else {
                    None
                }
            }
        }
    }

    /// Get exactly a word; otherwise None
    pub fn word(&self) -> Option<[u8; 4]> {
        match self {
            Bytes::Empty => None,
            Bytes::Byte(_) => None,
            Bytes::Word(w) => Some(*w),
            Bytes::Bytes(b) => {
                if b.len() == 4 {
                    Some([b[0], b[1], b[2], b[3]])
                } else {
                    None
                }
            }
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Bytes::Bytes(bytes)
    }

    fn bytes_copy(&self) -> Vec<u8> {
        match self {
            Bytes::Empty => vec![],
            Bytes::Byte(b) => vec![*b],
            Bytes::Word([a, b, c, d]) => vec![*a, *b, *c, *d],
            Bytes::Bytes(bytes) => bytes.clone(),
        }
    }

    pub fn append(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other;
            return;
        }
        *self = Self::Bytes(self.take().bytes());
        match self {
            Bytes::Bytes(bytes) => bytes.extend_from_slice(&other.bytes()),
            _ => unreachable!(),
        }
    }

    pub fn take(&mut self) -> Self {
        let mut ret = Bytes::Empty;
        std::mem::swap(self, &mut ret);
        ret
    }
}

/// IO methods
impl Bytes {
    #[inline]
    pub async fn read_from(file: &mut impl ByteSource, size: usize) -> Result<Self, FileErr> {
        file.request_bytes(size).await
    }

    #[inline]
    pub fn write_to(self, file: &mut FileSink) -> Result<(), FileErr> {
        file.write(self)
    }
}

impl ByteSource for Bytes {
    type Future<'a> = Ready<Result<Bytes, FileErr>>;

    fn request_bytes<'a>(&'a mut self, size: usize) -> Self::Future<'a> {
        if size <= self.len() {
            ready(Ok(self.pop(size)))
        } else {
            ready(Err(FileErr::NotEnoughBytes))
        }
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Empty, Self::Empty) => true,
            (Self::Byte(l), Self::Byte(r)) => l == r,
            (Self::Word(l), Self::Word(r)) => l == r,
            (Self::Bytes(l), Self::Bytes(r)) => l == r,
            (left, right) => {
                if left.len() != right.len() {
                    false
                } else {
                    left.bytes_copy() == right.bytes_copy()
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bytes() {
        assert!(Bytes::Empty.is_empty());
        assert!(Bytes::Bytes(vec![]).is_empty());

        let mut bytes = Bytes::Empty;
        bytes.append(Bytes::Byte(1));
        assert_eq!(bytes.bytes(), vec![1]);

        let mut bytes = Bytes::Empty;
        bytes.append(Bytes::Bytes(vec![1, 2]));
        assert_eq!(bytes.bytes_copy(), vec![1, 2]);
        assert_eq!(bytes.pop(1).bytes(), vec![1]);
        assert_eq!(bytes.bytes_copy(), vec![2]);
        assert_eq!(bytes.pop(1).bytes(), vec![2]);
        assert!(bytes.is_empty());

        let mut bytes = Bytes::Byte(1);
        bytes.append(Bytes::Byte(2));
        assert_eq!(bytes.bytes_copy(), vec![1, 2]);
        bytes.append(Bytes::Byte(3));
        assert_eq!(bytes.bytes_copy(), vec![1, 2, 3]);
        bytes.append(Bytes::Byte(4));
        assert_eq!(bytes.bytes_copy(), vec![1, 2, 3, 4]);
        assert_eq!(bytes.pop(2).bytes(), vec![1, 2]);
        assert_eq!(bytes.bytes_copy(), vec![3, 4]);
        assert_eq!(bytes.pop(2).bytes(), vec![3, 4]);
        assert!(bytes.is_empty());

        let mut bytes = Bytes::Bytes(vec![1, 2, 3, 4]);
        bytes.append(Bytes::Bytes(vec![5, 6]));
        assert_eq!(bytes.bytes_copy(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(bytes.pop(3).bytes(), vec![1, 2, 3]);
        assert_eq!(bytes.bytes(), vec![4, 5, 6]);
    }

    #[test]
    fn test_byte_buffer() {
        let mut buffer = ByteBuffer::new();
        let mut bytes = Bytes::Empty;
        bytes.append(Bytes::Byte(1));
        buffer.append(bytes);
        assert_eq!(buffer.size(), 1);
        assert_eq!(buffer.consume(1).bytes(), vec![1]);
        assert_eq!(buffer.size(), 0);
        buffer.append(Bytes::Byte(1));
        buffer.append(Bytes::Bytes(vec![2, 3]));
        assert_eq!(buffer.size(), 3);
        assert_eq!(buffer.consume(2).bytes(), vec![1, 2]);
        assert_eq!(buffer.size(), 1);
        assert_eq!(buffer.consume(1).bytes(), vec![3]);
        assert!(buffer.is_empty());

        let mut buffer = ByteBuffer::new();
        buffer.append(Bytes::Bytes(vec![1]));
        buffer.append(Bytes::Bytes(vec![2, 3]));
        buffer.append(Bytes::Bytes(vec![4, 5, 6]));
        buffer.append(Bytes::Bytes(vec![7, 8, 9, 10]));
        assert_eq!(buffer.size(), 10);
        assert_eq!(buffer.consume(4).bytes(), vec![1, 2, 3, 4]);
        assert_eq!(buffer.consume(3).bytes(), vec![5, 6, 7]);
        assert_eq!(buffer.consume(2).bytes(), vec![8, 9]);
        assert_eq!(buffer.consume(1).bytes(), vec![10]);
    }
}
