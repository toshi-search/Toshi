pub mod error;
pub mod query;
pub mod results;

pub use crate::error::Error;

type Result<T> = std::result::Result<T, Error>;
