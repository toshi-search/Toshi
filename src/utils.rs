use futures::future;
use http::header::CONTENT_TYPE;
use http::{Response, StatusCode};
use hyper::Body;
use serde::Serialize;

use crate::handlers::ResponseFuture;
use crate::error::Error;
use crate::results::ErrorResponse;

pub fn with_body<T>(body: T) -> http::Response<Body>
where
    T: Serialize,
{
    let json = serde_json::to_vec::<T>(&body).unwrap();

    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap()
}

pub fn error_response(code: StatusCode, e: Error) -> http::Response<Body> {
    let mut resp = with_body(ErrorResponse::from(e));
    *resp.status_mut() = code;
    resp
}

pub fn empty_with_code(code: StatusCode) -> http::Response<Body> {
    Response::builder().status(code).body(Body::empty()).unwrap()
}

pub fn not_found() -> ResponseFuture {
    let not_found = empty_with_code(StatusCode::NOT_FOUND);
    Box::new(future::ok(not_found))
}

pub fn parse_path(path: &str) -> Vec<&str> {
    path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect()
}
