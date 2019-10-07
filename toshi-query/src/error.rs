use failure::Fail;
use serde::{Deserialize, Serialize};

#[derive(Debug, Fail, Serialize, Deserialize)]
pub enum Error {
    #[fail(display = "Error in query execution: '{}'", _0)]
    QueryError(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::QueryError(err.to_string())
    }
}
