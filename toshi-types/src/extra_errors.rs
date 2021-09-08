use crate::error::{Error, ErrorResponse};

impl From<Error> for http::Response<hyper::Body> {
    fn from(err: Error) -> Self {
        let body = ErrorResponse::new(err);
        let bytes = serde_json::to_vec(&body).unwrap();
        http::Response::new(hyper::Body::from(bytes))
    }
}
