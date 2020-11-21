#![forbid(unsafe_code)]
#![deny(future_incompatible, warnings)]
#![allow(clippy::cognitive_complexity)]

use std::sync::Arc;

use slog::Logger;

use toshi_types::FlatNamedDocument;

use crate::index::{IndexCatalog, SharedCatalog};
use crate::settings::Settings;

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
    let index_catalog = IndexCatalog::new(settings.clone()).unwrap();
    Arc::new(index_catalog)
}

#[cfg(not(debug_assertions))]
pub fn setup_logging_from_file(path: &str) -> Result<Logger> {
    use sloggers::{Config, LoggerConfig};
    let file = std::fs::read(path)?;
    toml::from_slice(&file)
        .map(|cfg: LoggerConfig| cfg.build_logger().expect("Bad Config Format"))
        .map_err(toshi_types::Error::TomlError)
}

#[cfg(debug_assertions)]
pub fn setup_logging_from_file(_: &str) -> Result<Logger> {
    use slog::{Drain, Level};
    let decorator = slog_term::TermDecorator::new().stdout().build();
    let format = slog_term::FullFormat::new(decorator)
        .use_local_timestamp()
        .use_original_order()
        .build()
        .fuse();
    let sink = slog_async::Async::new(format).build().filter_level(Level::Info).fuse();
    let filter = slog::o!("toshi" => "debug");
    Ok(Logger::root(sink, filter))
}
