use futures::future;
use http::header::CONTENT_TYPE;
use hyper::{Body, Response};

use crate::handlers::ResponseFuture;

const TOSHI_INFO: &[u8] = b"{\"name\":\"Toshi Search\",\"version\":\"0.1.1\"}";

pub fn root() -> ResponseFuture {
    let resp = Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(TOSHI_INFO))
        .map_err(Into::into);

    Box::new(future::result(resp))
}

#[cfg(test)]
mod tests {

    use super::*;
    use bytes::Buf;
    use tokio::prelude::*;

    #[test]
    fn test_root() -> Result<(), hyper::Error> {
        root()
            .wait()
            .unwrap()
            .into_body()
            .concat2()
            .map(|v| assert_eq!(v.bytes(), TOSHI_INFO))
            .wait()
    }
}
