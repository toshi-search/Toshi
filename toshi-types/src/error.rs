use failure::Fail;
use http::uri::InvalidUri;
use serde::{Deserialize, Serialize};
use tantivy::query::QueryParserError;
use tantivy::schema::DocParsingError;
use tantivy::TantivyError;

#[derive(Serialize)]
pub struct ErrorResponse {
    message: String,
}

#[derive(Debug, Fail, Serialize, Deserialize)]
pub enum Error {
    #[fail(display = "IO Error: {}", _0)]
    IOError(String),
    #[fail(display = "Unknown Field: '{}' queried", _0)]
    UnknownIndexField(String),
    #[fail(display = "Unknown Index: '{}' does not exist", _0)]
    UnknownIndex(String),
    #[fail(display = "Error in query execution: '{}'", _0)]
    QueryError(String),
    #[fail(display = "Failed to find known executor")]
    SpawnError,
    #[fail(display = "An unknown error occurred")]
    UnknownError,
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

impl From<Box<dyn::std::error::Error + Send + 'static>> for Error {
    fn from(err: Box<dyn::std::error::Error + Send + 'static>) -> Self {
        Error::IOError(err.description().to_owned())
    }
}

#[derive(Fail, Debug)]
pub enum ToshiClientError {
    #[fail(display = "Serde deserialization error: {}", _0)]
    JsonError(serde_json::Error),
    #[fail(display = "Isahc error: {}", _0)]
    IsahcError(String),
    #[fail(display = "IO Error: {}", _0)]
    UriError(InvalidUri),
}

impl From<InvalidUri> for ToshiClientError {
    fn from(e: InvalidUri) -> Self {
        ToshiClientError::UriError(e)
    }
}

impl From<serde_json::Error> for ToshiClientError {
    fn from(e: serde_json::Error) -> Self {
        ToshiClientError::JsonError(e)
    }
}

impl From<TantivyError> for Error {
    fn from(err: tantivy::Error) -> Self {
        Error::IOError(err.to_string())
    }
}

impl From<isahc::Error> for ToshiClientError {
    fn from(e: isahc::Error) -> Self {
        ToshiClientError::IsahcError(e.to_string())
    }
}
