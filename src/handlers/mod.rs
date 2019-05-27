use futures::Future;
use http::Response;
use hyper::Body;

pub use self::{bulk::BulkHandler, index::IndexHandler, search::SearchHandler, summary::SummaryHandler};

pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub type BaseFuture = Future<Item = Response<Body>, Error = hyper::Error> + Send;
pub type ResponseFuture = Box<BaseFuture>;
