pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, search::SearchHandler, summary::SummaryHandler};

use futures::Future;
use hyper::Body;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
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

pub type ResponseFuture = Box<Future<Item = hyper::Response<Body>, Error = failure::Error> + Send>;
