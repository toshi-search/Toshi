pub mod client;
pub mod error;
pub mod query;
pub mod server;

type Result<T> = std::result::Result<T, error::Error>;
