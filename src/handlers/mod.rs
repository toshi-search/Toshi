pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, root::RootHandler, search::SearchHandler, summary::SummaryHandler};

use super::Error;
use settings::Settings;

use serde::Serialize;
use serde_json;

#[derive(Extract)]
pub struct IndexPath {
    index: String,
}

#[derive(Extract)]
pub struct QueryOptions {
    #[serde(default = "Settings::default_pretty")]
    pretty: bool,
}

#[derive(Response)]
pub struct CreatedResponse;

pub struct ErrorResponse {
    reason: String,
}

impl ErrorResponse {
    pub fn new(reason: &str) -> Self {
        ErrorResponse {
            reason: reason.to_string(),
        }
    }
}

fn to_json<T: Serialize>(result: T, pretty: bool) -> Vec<u8> {
    if pretty {
        serde_json::to_vec_pretty(&result).unwrap()
    } else {
        serde_json::to_vec(&result).unwrap()
    }
}
