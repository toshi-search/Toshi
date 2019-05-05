use futures::future;
use http::header::CONTENT_TYPE;
use hyper::{Body, Response};
use serde::Serialize;

use crate::handlers::ResponseFuture;

#[derive(Clone, Debug)]
pub struct RootHandler(ToshiInfo);

#[derive(Debug, Clone, Serialize)]
struct ToshiInfo {
    name: &'static str,
    version: &'static str,
}

impl RootHandler {
    pub fn new(version: &'static str) -> Self {
        RootHandler(ToshiInfo {
            version,
            name: "Toshi Search",
        })
    }

    pub fn call(&self) -> ResponseFuture {
        match serde_json::to_vec(&self.0) {
            Ok(v) => {
                let resp = Response::builder()
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(v))
                    .map_err(Into::into);

                Box::new(future::result(resp))
            }
            Err(e) => panic!("{:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::settings::VERSION;

    use super::*;

    #[test]
    fn test_root() {
        //        let handler = RootHandler::new(VERSION);
        //        assert_eq!(handler.root().unwrap().version, VERSION)
    }
}
