use hyper::Body;
use serde::Serialize;

use toshi_types::error::{Error, ErrorResponse};

pub fn with_body<T>(body: T) -> hyper::Response<Body>
where
    T: Serialize,
{
    let json = serde_json::to_vec::<T>(&body).unwrap();

    hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap()
}

pub fn error_response(code: hyper::StatusCode, e: Error) -> hyper::Response<Body> {
    let mut resp = with_body(ErrorResponse { message: e.to_string() });
    *resp.status_mut() = code;
    resp
}

pub fn empty_with_code(code: hyper::StatusCode) -> hyper::Response<Body> {
    hyper::Response::builder().status(code).body(Body::empty()).unwrap()
}

pub async fn not_found() -> Result<hyper::Response<Body>, hyper::Error> {
    Ok(empty_with_code(hyper::StatusCode::NOT_FOUND))
}

pub fn parse_path(path: &str) -> Vec<&str> {
    path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect()
}

#[cfg(test)]
mod tests {

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_path() {
        let root = "/";
        let one = "/path";
        let two = "/path/two";

        let parsed_root = parse_path(root);
        let parsed_one = parse_path(one);
        let parsed_two = parse_path(two);
        assert_eq!(parsed_root.len(), 0);
        assert_eq!(parsed_one.len(), 1);
        assert_eq!(parsed_one[0], "path");
        assert_eq!(parsed_two.len(), 2);
        assert_eq!(parsed_two[0], "path");
        assert_eq!(parsed_two[1], "two");
    }
}
