#[cfg(feature = "runtime-tokio")]
pub use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[cfg(feature = "runtime-async-std")]
pub use async_std::{
    fs::{File, OpenOptions},
    io::{ReadExt as AsyncReadExt, WriteExt as AsyncWriteExt, prelude::SeekExt as AsyncSeekExt},
};

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
mod no_rt_file;
#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use no_rt_file::*;

pub use std::io::SeekFrom;
