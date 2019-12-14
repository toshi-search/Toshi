use hyper::Body;

pub use {bulk::*, index::*, root::*, search::*, summary::*};

pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub type ResponseFuture = Result<hyper::Response<Body>, hyper::Error>;
