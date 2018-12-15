use tokio::prelude::*;

#[derive(Clone, Debug)]
pub struct RootHandler(ToshiInfo);

#[derive(Response, Serialize)]
#[web(status = "200")]
struct ToshiInfo {
    name: String,
    version: String,
}

impl RootHandler {
    pub fn new(version: &str) -> Self {
        RootHandler(ToshiInfo {
            version,
            name: "Toshi Search".into(),
        })
    }
}

impl_web! {
    impl RootHandler {
        #[get("/")]
        #[content_type("application/json")]
        fn root(&self) -> Result<ToshiInfo, ()> {
            Ok(&self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gotham::test::TestServer;
    use settings::VERSION;

    use tower_web::ServiceBuilder;

    #[test]
    fn test_tower() {
        ServiceBuilder::new().resource(RootHandler::new(VERSION)).run(&([127, 0, 0, 1], 8080).into()).unwrap();
    }


    #[test]
    fn test_root() {
//        let handler = RootHandler::new(VERSION);
//        let test_server = TestServer::new(handler).unwrap();
//        let client = test_server.client();
//
//        let req = client.get("http://localhost").perform().unwrap();
//        assert_eq!(req.status(), StatusCode::OK);
//        assert_eq!(req.read_utf8_body().unwrap(), r#"{"name":"Toshi Search","version":"0.1.1"}"#);
    }
}
