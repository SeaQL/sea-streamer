mod consumer;
mod error;
mod host;
mod producer;
mod streamer;

pub use consumer::*;
pub use error::*;
pub use host::*;
pub use producer::*;
pub use streamer::*;

macro_rules! impl_into_string {
    ($name:ident) => {
        impl From<$name> for String {
            fn from(o: $name) -> Self {
                o.as_str().to_owned()
            }
        }
    };
}

pub(crate) use impl_into_string;
