pub mod error;
pub mod query;
pub mod results;
pub mod search;

pub use crate::error::Error;
pub use search::search_index;

pub use query::Search;
pub use results::SearchResults;

type Result<T> = std::result::Result<T, Error>;
