#![warn(unused_extern_crates)]
extern crate clap;
extern crate config;
extern crate crossbeam;
#[macro_use]
extern crate failure;
extern crate futures;
extern crate gotham;

extern crate hyper;
extern crate log;
extern crate mime;
extern crate num_cpus;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate systemstat;
extern crate tantivy;
extern crate tokio;
extern crate uuid;
#[macro_use]
extern crate tower_web;

use failure::Fail;
use log::*;
use tantivy::query::QueryParserError;
use tantivy::schema::DocParsingError;
use tantivy::Error as TError;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "IO Error: {}", _0)]
    IOError(String),
    #[fail(display = "Unknown Field: '{}' queried", _0)]
    UnknownIndexField(String),
    #[fail(display = "Unknown Index: '{}' does not exist", _0)]
    UnknownIndex(String),
    #[fail(display = "Query Parse Error: {}", _0)]
    QueryError(String),
}

impl From<TError> for Error {
    fn from(err: tantivy::Error) -> Self {
        match err {
            TError::CorruptedFile(p) | TError::PathDoesNotExist(p) | TError::FileAlreadyExists(p) => Error::IOError(format!("{:?}", p)),
            TError::IOError(e) => Error::IOError(e.to_string()),
            TError::SchemaError(e) => Error::UnknownIndex(e.to_string()),
            TError::InvalidArgument(e) | TError::ErrorInThread(e) => Error::IOError(e),
            TError::Poisoned => Error::IOError("Poisoned".to_string()),
            TError::LockFailure(e) => Error::IOError(format!("Failed to acquire lock: {:?}", e)),
            TError::FastFieldError(_) => Error::IOError("Fast Field Error".to_string()),
            TError::IndexAlreadyExists => Error::IOError("Index Already Exists".into()),
        }
    }
}

impl From<QueryParserError> for Error {
    fn from(qpe: QueryParserError) -> Self {
        match qpe {
            QueryParserError::SyntaxError => Error::QueryError("Syntax error in query".to_string()),
            QueryParserError::FieldDoesNotExist(e) => Error::UnknownIndexField(e),
            QueryParserError::FieldNotIndexed(e) | QueryParserError::FieldDoesNotHavePositionsIndexed(e) => {
                Error::QueryError(format!("Query to unindexed field '{}'", e))
            }
            QueryParserError::ExpectedInt(e) => Error::QueryError(e.to_string()),
            QueryParserError::NoDefaultFieldDeclared | QueryParserError::RangeMustNotHavePhrase => {
                Error::QueryError("No default field declared for query".to_string())
            }
            QueryParserError::AllButQueryForbidden => Error::QueryError("Cannot have queries only exclude documents".to_string()),
            QueryParserError::UnknownTokenizer(e1, _) => Error::QueryError(e1),
        }
    }
}

impl From<DocParsingError> for Error {
    fn from(err: DocParsingError) -> Self {
        match err {
            DocParsingError::NotJSON(e) => Error::IOError(e),
            DocParsingError::NoSuchFieldInSchema(e) => Error::UnknownIndexField(e),
            DocParsingError::ValueError(e, _) => Error::IOError(e),
        }
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::IOError(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::IOError(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::QueryError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

mod handle;
mod handlers;
mod query;
mod results;

pub mod cluster;
pub mod commit;
pub mod index;
//pub mod router;
pub mod settings;
