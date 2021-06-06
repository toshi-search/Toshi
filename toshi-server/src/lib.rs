#![forbid(unsafe_code)]
#![deny(future_incompatible)]
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
    use sloggers::types::*;
    use sloggers::Build;
    let log = sloggers::terminal::TerminalLoggerBuilder::new()
        .format(Format::Full)
        .level(Severity::Info)
        .timezone(TimeZone::Local)
        .build()
        .map_err(anyhow::Error::from)?;

    Ok(log)
}

#[cfg(feature = "extra_tokenizers")]
pub fn register_tokenizers(idx: tantivy::Index) -> tantivy::Index {
    let schema = idx.schema();
    let has_tokenizer = schema.fields().find(|(_, entry)| match entry.field_type() {
        tantivy::schema::FieldType::Str(ref opts) => opts
            .get_indexing_options()
            .map(|to| to.tokenizer() == cang_jie::CANG_JIE)
            .unwrap_or(false),
        _ => false,
    });
    if has_tokenizer.is_some() {
        let tokenizer = cang_jie::CangJieTokenizer::default();
        idx.tokenizers().register(cang_jie::CANG_JIE, tokenizer)
    }
    idx
}

#[cfg(not(feature = "extra_tokenizers"))]
pub fn register_tokenizers(idx: tantivy::Index) -> tantivy::Index {
    idx
}
