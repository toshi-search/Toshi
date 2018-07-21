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

#[cfg_attr(test, macro_use)]
extern crate tantivy;

#[macro_use]
extern crate lazy_static;
extern crate config;
extern crate crossbeam_channel;
extern crate pretty_env_logger;

use tantivy::query::QueryParserError;
use tantivy::schema::DocParsingError;
use tantivy::ErrorKind;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        IOError(err: String) {
            display("IO Error: {}", err)
        }
        UnknownIndexField(err: String) {
            display("Unknown Field: '{}' queried", err)
        }
        UnknownIndex(err: String) {
            display("Unknown Index: '{}' queried", err)
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
                Error::IOError(format!("{:?}", p))
            }
            ErrorKind::IOError(e) => Error::IOError(e.to_string()),
            ErrorKind::SchemaError(e) => Error::UnknownIndex(e.to_string()),
            e => Error::TantivyError(e.to_string()),
        }
    }
}

impl From<QueryParserError> for Error {
    fn from(qpe: QueryParserError) -> Error {
        match qpe {
            QueryParserError::SyntaxError => Error::QueryError(String::from("Syntax error in query")),
            QueryParserError::FieldDoesNotExist(e)
            | QueryParserError::FieldNotIndexed(e)
            | QueryParserError::FieldDoesNotHavePositionsIndexed(e) => Error::UnknownIndexField(e),
            QueryParserError::ExpectedInt(e) => Error::QueryError(e.to_string()),
            QueryParserError::NoDefaultFieldDeclared => Error::QueryError(String::from("No default field declared for query")),
            QueryParserError::AllButQueryForbidden => Error::QueryError(String::from("Cannot have queries only exclude documents")),
            e => Error::TantivyError(format!("{:?}", e)),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error { Error::IOError(err.to_string()) }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Error { Error::IOError(err.to_string()) }
}

impl From<DocParsingError> for Error {
    fn from(err: DocParsingError) -> Error {
        match err {
            DocParsingError::NotJSON(e) => Error::IOError(e),
            DocParsingError::ValueError(s, _) => Error::IOError(s),
            DocParsingError::NoSuchFieldInSchema(e) => Error::UnknownIndexField(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

mod handlers;
pub mod index;
pub mod router;
pub mod settings;
