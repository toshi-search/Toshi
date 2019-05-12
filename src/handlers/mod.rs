pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, search::SearchHandler, summary::SummaryHandler};

pub type ResponseFuture = Box<futures::Future<Item = hyper::Response<hyper::Body>, Error = failure::Error> + Send>;
