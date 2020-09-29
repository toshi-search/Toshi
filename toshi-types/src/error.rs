//! This contains Toshi's server errors, if you are looking for the hyper or tonic errors they are
//! located in the [`crate::extra_errors`]: extra_errors
//! module

use std::fmt::Debug;

use failure::Fail;
use serde::{Deserialize, Serialize};
use tantivy::directory::error::OpenDirectoryError;
use tantivy::query::QueryParserError;
use tantivy::schema::DocParsingError;
use tantivy::TantivyError;
use thiserror::Error;

/// The type returned when an error occurs with a query
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// The human-readable message given back
    pub message: String,
}

impl ErrorResponse {
    /// Create an error response from anything that implements ToString
    pub fn new<M: ToString>(message: M) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

/// Toshi's base error types
#[derive(Debug, Error)]
pub enum Error {
    /// IO error that deals with anything related to reading from disk or network communications
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    /// Unlikely error related to Slog
    #[error("IO Error: {0}")]
    SlogError(#[from] slog::Error),
    /// A query tried to reference a field that does not exist
    #[error("Unknown Field: '{0}' queried")]
    UnknownIndexField(String),
    /// A query tried to query an index that does not exist
    #[error("Unknown Index: '{0}' does not exist")]
    UnknownIndex(String),
    /// A query that had a syntax error or was otherwise not valid
    #[error("Error in query execution: '{0}'")]
    QueryError(String),
    /// This should never occur and is a bug that should be reported
    #[error("Failed to find known executor")]
    SpawnError,
    /// oOoOOoOOOoOOo Spooooky ghosts, maybe, we don't know.
    #[error("An unknown error occurred")]
    UnknownError,
    /// This should never occur and is a bug that should be reported
    #[error("Thread pool is poisoned")]
    PoisonedError,
    /// An error occured in Toshi's internal RPC communications
    #[error("An RPC error occurred: '{0}'")]
    RPCError(String),
    /// Any Error related to Tantivy
    #[error("Error in Index: '{0}'")]
    TantivyError(#[from] anyhow::Error),
    /// Any error related to serde_json
    #[error("Error Parsing Json: '{0}'")]
    JsonParsing(#[from] serde_json::Error),
    /// Any error related to Hyper
    #[error("Http Error: '{0}'")]
    HyperError(#[from] hyper::Error),
    /// Any error related to http
    #[error("Http Crate Error: '{0}'")]
    HttpError(#[from] http::Error),
    /// When attempting to create an index that already exists
    #[error("Index: '{0}' already exists")]
    AlreadyExists(String),
    /// When an invalid log config is provided
    #[error("Error Deserializing Error: '{0}'")]
    TomlError(toml::de::Error),
}

impl From<OpenDirectoryError> for Error {
    fn from(err: OpenDirectoryError) -> Self {
        Error::TantivyError(anyhow::Error::new(err.compat()))
    }
}

impl From<QueryParserError> for Error {
    fn from(err: QueryParserError) -> Self {
        Error::TantivyError(anyhow::Error::new(err.compat()))
    }
}

impl From<DocParsingError> for Error {
    fn from(err: DocParsingError) -> Self {
        Error::TantivyError(anyhow::Error::new(err.compat()))
    }
}

impl From<TantivyError> for Error {
    fn from(err: TantivyError) -> Self {
        Error::TantivyError(anyhow::Error::new(err.compat()))
    }
}
