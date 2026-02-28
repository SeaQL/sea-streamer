#[cfg(feature = "runtime-tokio")]
pub use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[cfg(feature = "runtime-smol")]
pub use smol::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
mod no_rt_file;
#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-smol")))]
pub use no_rt_file::*;

pub use std::io::SeekFrom;
