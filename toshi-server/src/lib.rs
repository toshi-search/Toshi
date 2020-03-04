#![forbid(unsafe_code)]
#![deny(future_incompatible)]
#![allow(clippy::cognitive_complexity)]

use std::path::PathBuf;
use std::sync::Arc;

use slog::{Drain, Logger};

use toshi_types::FlatNamedDocument;

use crate::index::{IndexCatalog, SharedCatalog};
use crate::settings::Settings;

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
pub type AddDocument = toshi_types::AddDocument<serde_json::Value>;
pub type SearchResults = toshi_types::SearchResults<FlatNamedDocument>;

pub fn setup_catalog(settings: &Settings) -> SharedCatalog {
    let path = PathBuf::from(&settings.path);
    let index_catalog = IndexCatalog::new(path, settings.clone()).unwrap();
    Arc::new(index_catalog)
}

pub fn setup_logging() -> Logger {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).use_local_timestamp().build().fuse();
    let async_drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(async_drain, slog::o!("toshi" => "toshi"))
}
