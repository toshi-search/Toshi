pub mod bulk;
pub mod index;
pub mod root;
pub mod search;
pub mod summary;

pub use self::{bulk::BulkHandler, index::IndexHandler, root::RootHandler, search::SearchHandler, summary::SummaryHandler};

use super::Error;
use settings::Settings;

#[derive(Extract, Deserialize)]
pub struct IndexPath {
    index: String,
}

#[derive(Extract, Deserialize)]
pub struct QueryOptions {
    #[serde(default = "Settings::default_pretty")]
    pretty: bool,
}

#[derive(Response)]
pub struct CreatedResponse;