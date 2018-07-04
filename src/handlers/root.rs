use super::*;

use futures::future;
use std::io::Result;

#[derive(Clone, Debug)]
pub struct RootHandler(String);

impl RootHandler {
    pub fn new(version: String) -> Self { RootHandler(version) }
}

impl Handler for RootHandler {
    fn handle(self, state: State) -> Box<HandlerFuture> {
        let body = self.0.as_bytes().to_vec();
        let resp = create_response(&state, StatusCode::Ok, Some((body, mime::TEXT_HTML)));
        Box::new(future::ok((state, resp)))
    }
}

impl NewHandler for RootHandler {
    type Instance = Self;

    fn new_handler(&self) -> Result<Self::Instance> { Ok(self.clone()) }
}
