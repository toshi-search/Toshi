pub mod cluster;
pub mod commit;
pub mod error;
pub mod handle;
pub mod handlers;
pub mod index;
pub mod indexing;
pub mod query;
pub mod results;
pub mod router;
pub mod settings;
pub mod shutdown;
pub mod support;
pub mod utils;

pub type Result<T> = std::result::Result<T, crate::error::Error>;
