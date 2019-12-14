use std::fmt::Debug;

use hyper::Body;
use serde::{Deserialize, Serialize};
use tantivy::query::QueryParserError;
use tantivy::schema::DocParsingError;
use tantivy::TantivyError;
use thiserror::Error;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

impl ErrorResponse {
    pub fn new<M: std::fmt::Display>(message: M) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum Error {
    #[error("IO Error: {0}")]
    IOError(String),
    #[error("Unknown Field: '{0}' queried")]
    UnknownIndexField(String),
    #[error("Unknown Index: '{0}' does not exist")]
    UnknownIndex(String),
    #[error("Error in query execution: '{0}'")]
    QueryError(String),
    #[error("Failed to find known executor")]
    SpawnError,
    #[error("An unknown error occurred")]
    UnknownError,
    #[error("Thread pool is poisoned")]
    PoisonedError,
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
        match e {
            TantivyError::IOError(e) => Error::IOError(e.to_string()),
            TantivyError::DataCorruption(e) => Error::IOError(format!("Data corruption: {:?}", e)),
            TantivyError::PathDoesNotExist(e) => Error::IOError(format!("{:?}", e)),
            TantivyError::FileAlreadyExists(e) => Error::IOError(format!("{:?}", e)),
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

impl From<Error> for hyper::Response<Body> {
    fn from(err: Error) -> Self {
        let body = ErrorResponse::new(err);
        let bytes = serde_json::to_vec(&body).unwrap();
        hyper::Response::new(Body::from(bytes))
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::QueryError(err.to_string())
    }
}
