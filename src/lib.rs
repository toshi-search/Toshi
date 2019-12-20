#![forbid(unsafe_code)]
#![deny(future_incompatible)]

use std::collections::BTreeMap;

use tantivy::schema::Value;

use toshi_types::client::SearchResults as SD;
use toshi_types::server::AddDocument as AD;

pub mod cluster;
pub mod commit;
pub mod handle;
pub mod handlers;
pub mod index;
pub mod router;
pub mod settings;
pub mod shutdown;
pub mod support;
pub mod utils;

pub type Result<T> = std::result::Result<T, toshi_types::error::Error>;
pub type AddDocument = AD<serde_json::Value>;
pub type SearchResults = SD<BTreeMap<String, Vec<Value>>>;
