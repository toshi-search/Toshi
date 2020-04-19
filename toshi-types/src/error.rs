//! This contains Toshi's server errors, if you are looking for the hyper or tonic errors they are
//! located in the [`crate::extra_errors`]: extra_errors
//! module

use std::fmt::Debug;

use serde::{Deserialize, Serialize};
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
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum Error {
    /// IO error that deals with anything related to reading from disk or network communications
    #[error("IO Error: {0}")]
    IOError(String),
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
}

impl From<QueryParserError> for Error {
    fn from(qpe: QueryParserError) -> Self {
        match qpe {
            QueryParserError::SyntaxError => Error::QueryError("Syntax error in query".into()),
            QueryParserError::FieldDoesNotExist(e) => Error::UnknownIndexField(e),
            QueryParserError::FieldNotIndexed(e) => Error::QueryError(format!("Query on un-indexed field {}", e)),
            QueryParserError::FieldDoesNotHavePositionsIndexed(e) => {
                Error::QueryError(format!("Field {} does not have positions indexed", e))
            }
            QueryParserError::ExpectedInt(e) => Error::QueryError(e.to_string()),
            QueryParserError::ExpectedFloat(e) => Error::QueryError(e.to_string()),
            QueryParserError::NoDefaultFieldDeclared | QueryParserError::RangeMustNotHavePhrase => {
                Error::QueryError("No default field declared for query".into())
            }
            QueryParserError::AllButQueryForbidden => Error::QueryError("Cannot have queries that only exclude documents".into()),
            QueryParserError::UnknownTokenizer(field, tok) => Error::QueryError(format!("Unknown tokenizer {} for field {}", tok, field)),
            QueryParserError::DateFormatError(p) => Error::QueryError(p.to_string()),
        }
    }
}

impl From<DocParsingError> for Error {
    fn from(err: DocParsingError) -> Self {
        match err {
            DocParsingError::NotJSON(e) => Error::IOError(format!("Document: '{}' is not valid JSON", e)),
            DocParsingError::NoSuchFieldInSchema(e) => Error::UnknownIndexField(e),
            DocParsingError::ValueError(e, r) => {
                Error::IOError(format!("A value in the JSON '{}' could not be parsed, reason: {:?}", e, r))
            }
        }
    }
}

impl From<TantivyError> for Error {
    fn from(e: TantivyError) -> Self {
        use tantivy::directory::error::Incompatibility;
        match e {
            TantivyError::IOError(e) => Error::IOError(e.to_string()),
            TantivyError::IncompatibleIndex(ie) => match ie {
                Incompatibility::IndexMismatch {
                    library_version,
                    index_version,
                } => Error::IOError(format!(
                    "Index incompatible with version of Tantivy, library: {:?}, index: {:?}",
                    library_version, index_version
                )),
                Incompatibility::CompressionMismatch {
                    library_compression_format,
                    index_compression_format,
                } => Error::IOError(format!(
                    "Incompatible compression format, library: {}, index: {}",
                    library_compression_format, index_compression_format
                )),
            },
            TantivyError::DataCorruption(e) => Error::IOError(format!("Data corruption: {:?}", e)),
            TantivyError::PathDoesNotExist(e) => Error::IOError(format!("Path does not exist: {:?}", e)),
            TantivyError::FileAlreadyExists(e) => Error::IOError(format!("File already exists: {:?}", e)),
            TantivyError::IndexAlreadyExists => Error::IOError(e.to_string()),
            TantivyError::LockFailure(e, _) => Error::IOError(e.to_string()),
            TantivyError::Poisoned => Error::PoisonedError,
            TantivyError::InvalidArgument(e) => Error::IOError(e),
            TantivyError::ErrorInThread(e) => Error::IOError(e),
            TantivyError::SchemaError(e) => Error::QueryError(e),
            TantivyError::SystemError(_) => Error::UnknownError,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IOError(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::QueryError(err.to_string())
    }
}

impl From<slog::Error> for Error {
    fn from(e: slog::Error) -> Self {
        Error::IOError(e.to_string())
    }
}
