use futures::future;

use super::*;

#[derive(Clone, Debug)]
pub struct RootHandler(String);

impl RootHandler {
    pub fn new(version: String) -> Self { RootHandler(version) }
}

impl Handler for RootHandler {
    fn handle(self, state: State) -> Box<HandlerFuture> {
        let body = self.0.into_bytes();
        let resp = create_response(&state, StatusCode::OK, mime::TEXT_HTML, body);
        Box::new(future::ok((state, resp)))
    }
}

new_handler!(RootHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use gotham::test::TestServer;
    use settings::VERSION;

    #[test]
    fn test_root() {
        let handler = RootHandler(VERSION.to_string());
        let test_server = TestServer::new(handler).unwrap();
        let client = test_server.client();

        let req = client.get("http://localhost").perform().unwrap();
        assert_eq!(req.read_utf8_body().unwrap(), "0.1.0");
    }
}
