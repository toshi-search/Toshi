#[macro_use]
extern crate gotham_derive;
extern crate gotham;
extern crate hyper;
extern crate mime;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate log;

#[macro_use]
extern crate quick_error;
extern crate futures;
#[macro_use]
extern crate tantivy;

#[macro_use]
extern crate lazy_static;
extern crate config;
extern crate pretty_env_logger;

use tantivy::query::QueryParserError;
use tantivy::ErrorKind;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        IOError(err: String) {
            display("IO Error: {}", err)
        }
        UnknownIndexField(err: String) {
            display("Unknown Field: {} Queried", err)
        }
        UnknownIndex(err: String) {
            display("Unknown Index: {} queried", err)
        }
        TantivyError(err: String) {
            display("Error with Tantivy: {}", err)
        }
        QueryError(err: String) {
            display("Query Parse Error: {}", err)
        }
    }
}

impl From<tantivy::Error> for Error {
    fn from(err: tantivy::Error) -> Error {
        match err.0 {
            ErrorKind::CorruptedFile(p) | ErrorKind::PathDoesNotExist(p) | ErrorKind::FileAlreadyExists(p) => {
                Error::IOError(format!("{:?}", p)).into()
            }
            ErrorKind::IOError(e) => Error::IOError(e.to_string()).into(),
            ErrorKind::SchemaError(e) => Error::UnknownIndex(e.to_string()).into(),
            e => Error::TantivyError(e.to_string()).into(),
        }
    }
}

impl From<QueryParserError> for Error {
    fn from(qpe: QueryParserError) -> Error {
        match qpe {
            QueryParserError::SyntaxError => Error::QueryError(String::from("Syntax error in query")).into(),
            QueryParserError::FieldDoesNotExist(e) => Error::UnknownIndexField(e).into(),
            QueryParserError::NoDefaultFieldDeclared => Error::QueryError(String::from("No default field declared for query")).into(),
            QueryParserError::AllButQueryForbidden => Error::QueryError(String::from("Cannot have queries only exclude documents")).into(),
            QueryParserError::FieldNotIndexed(e) => Error::QueryError(e).into(),
            QueryParserError::FieldDoesNotHavePositionsIndexed(e) => Error::QueryError(e).into(),
            _ => Error::TantivyError(String::from("An unknown error occured in query parsing")).into(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error { Error::IOError(err.to_string()).into() }
}

pub type Result<T> = std::result::Result<T, Error>;

mod handlers;
mod index;
pub mod router;
pub mod settings;
