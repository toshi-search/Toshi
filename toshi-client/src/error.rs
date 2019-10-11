use failure::Fail;
use http::uri::InvalidUri;

#[derive(Fail, Debug)]
pub enum ToshiClientError {
    #[fail(display = "Serde deserialization error: {}", _0)]
    JsonError(serde_json::Error),
    #[fail(display = "Isahc error: {}", _0)]
    IsahcError(String),
    #[fail(display = "IO Error: {}", _0)]
    UriError(InvalidUri),
}

impl From<isahc::Error> for ToshiClientError {
    fn from(e: isahc::Error) -> Self {
        ToshiClientError::IsahcError(e.to_string())
    }
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
