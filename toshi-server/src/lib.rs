#![forbid(unsafe_code)]
#![deny(future_incompatible)]
#![allow(clippy::cognitive_complexity)]

use std::collections::BTreeMap;

use tantivy::schema::Value;

use toshi_types::AddDocument as AD;
use toshi_types::SearchResults as SD;

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

pub type Result<T> = std::result::Result<T, toshi_types::Error>;
pub type AddDocument = AD<serde_json::Value>;
pub type SearchResults = SD<BTreeMap<String, Vec<Value>>>;
