pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, root::RootHandler, search::SearchHandler, summary::SummaryHandler};

use futures::Future;
use hyper::Body;
use serde::{Deserialize, Serialize};
use tower_web::{Extract, Response};

#[derive(Extract, Serialize)]
pub struct QueryOptions {
    #[allow(unused)]
    pretty: Option<i32>,
}

#[derive(Serialize)]
#[allow(dead_code)]
pub struct ErrorResponse {
    message: String,
    uri: String,
}

impl ErrorResponse {
    #[allow(dead_code)]
    pub fn new(message: String, uri: String) -> Self {
        Self { message, uri }
    }
}

#[derive(Response, Debug)]
#[web(status = "201")]
pub struct CreatedResponse;

pub type ResponseFuture = Box<Future<Item = hyper::Response<Body>, Error = failure::Error> + Send>;
