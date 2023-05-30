#[cfg(feature = "runtime-tokio")]
pub use tokio::sync::Mutex as AsyncMutex;

#[cfg(feature = "runtime-async-std")]
pub use async_std::sync::Mutex as AsyncMutex;

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
mod no_rt_mutex {
    use std::ops::{Deref, DerefMut};

    pub struct AsyncMutex<T> {
        m: std::marker::PhantomData<T>,
    }

    impl<T> AsyncMutex<T> {
        pub fn new(_: T) -> Self {
            Self {
                m: Default::default(),
            }
        }

        pub async fn lock(&self) -> Self {
            Self {
                m: Default::default(),
            }
        }
    }

    impl<T> Deref for AsyncMutex<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            unimplemented!("Please enable a runtime")
        }
    }

    impl<T> DerefMut for AsyncMutex<T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unimplemented!("Please enable a runtime")
        }
    }
}

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
pub use no_rt_mutex::*;
