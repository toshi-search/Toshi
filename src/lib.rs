use failure::Fail;
use tantivy::query::QueryParserError;
use tantivy::schema::DocParsingError;
use tantivy::TantivyError;

#[derive(Debug, Fail, Clone)]
pub enum Error {
    #[fail(display = "IO Error: {}", _0)]
    IOError(String),
    #[fail(display = "Unknown Field: '{}' queried", _0)]
    UnknownIndexField(String),
    #[fail(display = "Unknown Index: '{}' does not exist", _0)]
    UnknownIndex(String),
    #[fail(display = "Query Parse Error: {}", _0)]
    QueryError(String),
    #[fail(display = "Failed to find known executor")]
    SpawnError,
}

impl From<TantivyError> for Error {
    fn from(err: tantivy::Error) -> Self {
        match err {
            TantivyError::PathDoesNotExist(e) | TantivyError::FileAlreadyExists(e) => Error::IOError(e.display().to_string()),
            TantivyError::IOError(e) => Error::IOError(e.to_string()),
            TantivyError::SystemError(e) => Error::IOError(e),
            TantivyError::DataCorruption(e) => Error::IOError(format!("Data Corruption: '{:?}'", e)),
            TantivyError::SchemaError(e) => Error::UnknownIndex(e.to_string()),
            TantivyError::InvalidArgument(e) | TantivyError::ErrorInThread(e) => Error::IOError(e),
            TantivyError::Poisoned => Error::IOError("Poisoned".into()),
            TantivyError::LockFailure(i, e) => Error::IOError(format!("Failed to acquire lock: {:?}", e)),
            TantivyError::FastFieldError(e) => Error::IOError(format!("Fast Field Error: {:?}", e)),
            TantivyError::IndexAlreadyExists => Error::IOError("Index Already Exists".into()),
        }
    }
}

impl From<QueryParserError> for Error {
    fn from(qpe: QueryParserError) -> Self {
        match qpe {
            QueryParserError::SyntaxError => Error::QueryError("Syntax error in query".into()),
            QueryParserError::FieldDoesNotExist(e) => Error::UnknownIndexField(e),
            QueryParserError::FieldNotIndexed(e) | QueryParserError::FieldDoesNotHavePositionsIndexed(e) => {
                Error::QueryError(format!("Query to unindexed field '{}'", e))
            }
            QueryParserError::ExpectedInt(e) => Error::QueryError(e.to_string()),
            QueryParserError::NoDefaultFieldDeclared | QueryParserError::RangeMustNotHavePhrase => {
                Error::QueryError("No default field declared for query".into())
            }
            QueryParserError::AllButQueryForbidden => Error::QueryError("Cannot have queries only exclude documents".into()),
            QueryParserError::UnknownTokenizer(e1, _) => Error::QueryError(e1),
            QueryParserError::DateFormatError(p) => Error::QueryError(p.to_string()),
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

impl From<Box<::std::error::Error + Send + 'static>> for Error {
    fn from(err: Box<::std::error::Error + Send + 'static>) -> Self {
        Error::IOError(err.description().to_owned())
    }
}

impl<T> From<crossbeam::channel::SendError<T>> for Error {
    fn from(err: crossbeam::channel::SendError<T>) -> Self {
        Error::IOError(err.to_string())
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
pub mod router;
pub mod settings;
pub mod shutdown;
pub mod support;
