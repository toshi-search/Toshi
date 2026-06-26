use bytes::Bytes;
use http_body_util::Full;

use crate::error::{Error, ErrorResponse};

impl From<Error> for http::Response<Full<Bytes>> {
    fn from(err: Error) -> Self {
        let body = ErrorResponse::new(err);
        let bytes = serde_json::to_vec(&body).unwrap();
        http::Response::new(Full::new(Bytes::from(bytes)))
    }
}
