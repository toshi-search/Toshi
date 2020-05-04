use http::uri::InvalidUri;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ToshiClientError {
    #[error("Serde deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[cfg(feature = "isahc")]
    #[error("Isahc error: {0}")]
    IsahcError(#[from] isahc::Error),

    #[cfg(feature = "hyper")]
    #[error("Hyper error: {0}")]
    HyperError(#[from] hyper::Error),

    #[error("Http Error: {0}")]
    HttpError(#[from] http::Error),

    #[error("IO Error: {0}")]
    UriError(#[from] InvalidUri),
}
