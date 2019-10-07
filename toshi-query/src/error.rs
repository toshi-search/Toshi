use failure::Fail;
use serde::{Deserialize, Serialize};
use tantivy::query::QueryParserError;
use tantivy::Error as TantivyError;

#[derive(Debug, Fail, Serialize, Deserialize)]
pub enum Error {
    #[fail(display = "IO Error: {}", _0)]
    IOError(String),
    #[fail(display = "Error in query execution: '{}'", _0)]
    QueryError(String),
    #[fail(display = "Unknown Field: '{}' queried", _0)]
    UnknownIndexField(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::QueryError(err.to_string())
    }
}

impl From<TantivyError> for Error {
    fn from(err: tantivy::Error) -> Self {
        Error::IOError(err.to_string())
    }
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
