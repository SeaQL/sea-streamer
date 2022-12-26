mod consumer;
mod producer;
mod streamer;

pub use consumer::*;
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
