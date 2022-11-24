use std::str::Utf8Error;

#[derive(Debug)]
pub struct Message {
    payload: Vec<u8>,
}

impl Message {
    pub fn as_str(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(&self.payload)
    }
}

pub trait Sendable {
    fn as_bytes(&self) -> &[u8];
}

impl Sendable for Message {
    fn as_bytes(&self) -> &[u8] {
        &self.payload
    }
}

impl Sendable for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Sendable for str {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}
