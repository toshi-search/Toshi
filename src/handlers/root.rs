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
        let body = self.0.into_bytes();
        let resp = create_response(&state, StatusCode::Ok, Some((body, mime::TEXT_HTML)));
        Box::new(future::ok((state, resp)))
    }
}

new_handler!(RootHandler);
