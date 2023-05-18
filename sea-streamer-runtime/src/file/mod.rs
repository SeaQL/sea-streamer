#[cfg(feature = "runtime-tokio")]
pub use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

#[cfg(feature = "runtime-async-std")]
pub use async_std::{
    fs::File,
    io::{prelude::SeekExt as AsyncSeekExt, ReadExt as AsyncReadExt},
};

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
mod no_rt_file;
#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use no_rt_file::*;

pub use std::io::SeekFrom;
