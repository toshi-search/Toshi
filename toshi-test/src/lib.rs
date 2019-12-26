use std::net::SocketAddr;

use bytes::Buf;

use futures::Future;
use http::uri::{Authority, Scheme};
use http::{Response, Uri};
use hyper::client::HttpConnector;

use hyper::{Body, Client};
use tantivy::schema::*;
use tantivy::{doc, Index};

pub static CONTENT_TYPE: &str = "application/json";

pub fn get_localhost() -> SocketAddr {
    "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
}

pub fn create_test_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let test_text = builder.add_text_field("test_text", STORED | TEXT);
    let test_int = builder.add_i64_field("test_i64", STORED | INDEXED);
    let test_unsign = builder.add_u64_field("test_u64", STORED | INDEXED);
    let test_unindexed = builder.add_text_field("test_unindex", STORED);
    let test_facet = builder.add_facet_field("test_facet");

    let schema = builder.build();
    let idx = Index::create_in_ram(schema);
    let mut writer = idx.writer(30_000_000).unwrap();

    writer.add_document(doc! { test_text => "Test Document 1", test_int => 2014i64,  test_unsign => 10u64, test_unindexed => "no", test_facet => Facet::from("/cat/cat2") });
    writer.add_document(doc! { test_text => "Test Dockument 2", test_int => -2015i64, test_unsign => 11u64, test_unindexed => "yes", test_facet => Facet::from("/cat/cat2") });
    writer.add_document(doc! { test_text => "Test Duckiment 3", test_int => 2016i64,  test_unsign => 12u64, test_unindexed => "noo", test_facet => Facet::from("/cat/cat3") });
    writer.add_document(doc! { test_text => "Test Document 4", test_int => -2017i64, test_unsign => 13u64, test_unindexed => "yess", test_facet => Facet::from("/cat/cat4") });
    writer.add_document(doc! { test_text => "Test Document 5", test_int => 2018i64,  test_unsign => 14u64, test_unindexed => "nooo", test_facet => Facet::from("/dog/cat2") });
    writer.commit().unwrap();

    idx
}

pub struct TestServer {
    client: Client<HttpConnector>,
    addr: SocketAddr,
}

impl TestServer {
    pub fn with_server<S>(server: S) -> Result<Self, hyper::Error>
    where
        S: Future<Output = Result<(), hyper::Error>> + Send + 'static,
    {
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        let client = Client::new();
        tokio::spawn(server);
        Ok(TestServer { addr, client })
    }

    #[inline]
    fn make_uri(&self, path: &str) -> Uri {
        let auth = Authority::from_maybe_shared(self.addr.to_string()).unwrap();
        Uri::builder()
            .path_and_query(path)
            .scheme(Scheme::HTTP)
            .authority(auth)
            .build()
            .expect("Invalid URI")
    }

    pub async fn read_body(resp: Response<Body>) -> Result<String, Box<dyn std::error::Error>> {
        let body = hyper::body::aggregate(resp.into_body()).await?;
        let b = body.bytes();
        Ok(String::from_utf8(b.to_vec())?)
    }

    pub async fn get(&mut self, path: &str) -> Result<Response<Body>, hyper::Error> {
        let uri = self.make_uri(path);
        let req = self.client.get(uri);
        tokio::spawn(req).await.expect("Join Error")
    }
}

#[cfg(test)]
pub mod tests {
    use std::convert::Infallible;

    use std::net::SocketAddr;

    use http::Response;
    use hyper::server::conn::AddrStream;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::Server;

    use crate::TestServer;

    pub async fn svc() -> Result<(), hyper::Error> {
        let make_svc = make_service_fn(|_: &AddrStream| {
            async {
                Ok::<_, Infallible>(service_fn(|_req| {
                    async { Ok::<_, Infallible>(Response::new(hyper::Body::from("Hello World"))) }
                }))
            }
        });
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        let serv = Server::bind(&addr).serve(make_svc);
        serv.await?;
        Ok(())
    }

    #[tokio::test]
    async fn make_test() -> Result<(), Box<dyn std::error::Error>> {
        let mut ts = TestServer::with_server(svc())?;
        let request = ts.get("/").await?;
        let response = TestServer::read_body(request).await?;
        println!("{:?}", response);
        Ok(())
    }
}
