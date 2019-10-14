use http::uri::InvalidUri;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ToshiClientError {
    #[error("Serde deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Isahc error: {0}")]
    IsahcError(String),
    #[error("IO Error: {0}")]
    UriError(#[from] InvalidUri),
}

impl From<isahc::Error> for ToshiClientError {
    fn from(e: isahc::Error) -> Self {
        ToshiClientError::IsahcError(e.to_string())
    }
}
