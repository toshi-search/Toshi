pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, root::RootHandler, search::SearchHandler, summary::SummaryHandler};

use serde::{Deserialize, Serialize};
use tower_web::{Extract, Response};

#[derive(Extract, Serialize)]
pub struct QueryOptions {
    #[allow(unused)]
    pretty: Option<i32>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    message: String,
    uri: String,
}

impl ErrorResponse {
    pub fn new(message: String, uri: String) -> Self {
        Self { message, uri }
    }
}

#[derive(Response, Debug)]
pub struct CreatedResponse;
