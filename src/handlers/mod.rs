pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, root::RootHandler, search::SearchHandler, summary::SummaryHandler};

use crate::settings::Settings;
use failure::Fail;
use futures::{future, future::FutureResult};
use hyper::{Body, Response, StatusCode};
use serde_derive::{Deserialize, Serialize};
use tower_web::{Extract, Response};

#[derive(Extract)]
pub struct QueryOptions {
    pretty: Option<i32>,
}

#[derive(Response, Debug)]
pub struct CreatedResponse;
