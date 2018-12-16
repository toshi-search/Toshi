pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, root::RootHandler, search::SearchHandler, summary::SummaryHandler};

use super::Error;

#[derive(Extract)]
pub struct QueryOptions {
    pretty: Option<i32>,
}

#[derive(Response)]
pub struct CreatedResponse;
