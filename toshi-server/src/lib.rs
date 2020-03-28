#![forbid(unsafe_code)]
#![deny(future_incompatible)]
#![allow(clippy::cognitive_complexity)]

use std::path::PathBuf;
use std::sync::Arc;

use slog::Logger;
use sloggers::{Config, LoggerConfig};

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
pub mod utils;

pub type Result<T> = std::result::Result<T, toshi_types::Error>;
pub type AddDocument = toshi_types::AddDocument<serde_json::Value>;
pub type SearchResults = toshi_types::SearchResults<FlatNamedDocument>;

pub fn setup_catalog(settings: &Settings) -> SharedCatalog {
    let path = PathBuf::from(&settings.path);
    let index_catalog = IndexCatalog::new(path, settings.clone()).unwrap();
    Arc::new(index_catalog)
}

pub fn setup_logging_from_file(path: &str) -> Result<Logger> {
    let file = std::fs::read(path)?;
    toml::from_slice(&file)
        .map(|cfg: LoggerConfig| cfg.build_logger().expect("Invalid Logger Config"))
        .map_err(|err| toshi_types::Error::IOError(err.to_string()))
}
