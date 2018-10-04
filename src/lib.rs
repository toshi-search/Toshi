#[macro_use]
extern crate gotham_derive;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
#[cfg_attr(test, macro_use)]
extern crate tantivy;
#[macro_use]
extern crate lazy_static;
extern crate capnp;
extern crate config;
extern crate crossbeam_channel;
extern crate futures;
extern crate gotham;
extern crate hyper;
extern crate mime;
extern crate pretty_env_logger;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate tokio_threadpool;

use tantivy::query::QueryParserError;
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
    fn from(err: tantivy::Error) -> Error {
        match err {
            TError::CorruptedFile(p) | TError::PathDoesNotExist(p) | TError::FileAlreadyExists(p) => Error::IOError(format!("{:?}", p)),
            TError::IOError(e) => Error::IOError(e.to_string()),
            TError::SchemaError(e) => Error::UnknownIndex(e.to_string()),
            TError::InvalidArgument(e) => Error::IOError(e),
            TError::Poisoned => Error::IOError("Poisoned".to_string()),
            TError::ErrorInThread(e) => Error::IOError(e),
            TError::LockFailure(e) => Error::IOError(format!("Failed to acquire lock: {:?}", e)),
            TError::FastFieldError(_) => Error::IOError("Fast Field Error".to_string()),
        }
    }
}

impl From<QueryParserError> for Error {
    fn from(qpe: QueryParserError) -> Error {
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

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error { Error::IOError(err.to_string()) }
}

pub type Result<T> = std::result::Result<T, Error>;

mod handlers;
mod results;
mod transaction;

pub mod commit;
pub mod index;
pub mod router;
pub mod settings;

#[allow(dead_code)]
pub mod wal_capnp {
    #[cfg(target_family = "windows")]
    include!(concat!(env!("OUT_DIR"), "\\proto", "\\wal_capnp.rs"));
    #[cfg(target_family = "unix")]
    include!(concat!(env!("OUT_DIR"), "/proto", "/wal_capnp.rs"));
}
