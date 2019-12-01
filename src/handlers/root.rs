use futures::future;
use http::header::CONTENT_TYPE;
use hyper::{Body, Response};

use crate::handlers::ResponseFuture;

#[inline]
fn toshi_info() -> String {
    format!("{{\"name\":\"Toshi Search\",\"version\":\"{}\"}}", clap::crate_version!())
}

pub fn root() -> ResponseFuture {
    let resp = Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(toshi_info()))
        .unwrap();

    Box::new(future::ok(resp))
}

#[cfg(test)]
mod tests {

    use super::*;
    use futures::{Future, Stream};

    #[test]
    fn test_root() -> Result<(), hyper::Error> {
        root()
            .wait()
            .unwrap()
            .into_body()
            .concat2()
            .map(|v| assert_eq!(String::from_utf8(v.to_vec()).unwrap(), toshi_info()))
            .wait()
    }
}
