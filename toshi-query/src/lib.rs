pub mod error;
pub mod query;

pub use query::*;

pub use crate::error::Error;

type Result<T> = std::result::Result<T, Error>;
