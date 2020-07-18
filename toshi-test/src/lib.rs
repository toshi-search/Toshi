use std::net::{SocketAddr, TcpListener};

use futures::future::Either;
use futures::Future;
use http::uri::{Authority, Scheme};
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, Response, Uri};
use serde::de::DeserializeOwned;
use tantivy::schema::*;
use tantivy::{doc, Index};

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

pub async fn wait_json<T: DeserializeOwned>(r: Response<Body>) -> T {
    let bytes = read_body(r).await.unwrap();
    serde_json::from_slice::<T>(bytes.as_bytes()).unwrap_or_else(|e| panic!("Could not deserialize JSON: {:?}", e))
}

pub fn cmp_float(a: f32, b: f32) -> bool {
    let abs_a = a.abs();
    let abs_b = b.abs();
    let diff = (a - b).abs();
    if diff == 0.0 {
        return true;
    } else if a == 0.0 || b == 0.0 || (abs_a + abs_b < std::f32::MIN_POSITIVE) {
        return diff < (std::f32::EPSILON * std::f32::MIN_POSITIVE);
    }
    diff / (abs_a + abs_b).min(std::f32::MAX) < std::f32::EPSILON
}

pub async fn read_body(resp: Response<Body>) -> Result<String, Box<dyn std::error::Error>> {
    let b = hyper::body::to_bytes(resp.into_body()).await?;
    Ok(String::from_utf8(b.to_vec())?)
}

pub struct TestServer {
    client: Client<HttpConnector>,
    addr: SocketAddr,
}

impl TestServer {
    pub fn new() -> Result<(TcpListener, Self), hyper::Error> {
        let listen = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listen.local_addr().unwrap();
        let client = Client::new();
        Ok((listen, TestServer { addr, client }))
    }

    #[inline]
    pub fn uri(&self, path: &str) -> Uri {
        let auth = Authority::from_maybe_shared(self.addr.to_string()).unwrap();
        Uri::builder()
            .path_and_query(path)
            .scheme(Scheme::HTTP)
            .authority(auth)
            .build()
            .expect("Invalid URI")
    }

    pub async fn get<S>(&self, req: Request<Body>, server: S) -> Result<Response<Body>, hyper::Error>
    where
        S: Future<Output = Result<(), hyper::Error>> + Send + 'static,
    {
        let client_req = self.client.request(req);
        let svr = tokio::spawn(server);
        let req = tokio::spawn(client_req);
        let svc = futures::future::try_select(Box::pin(svr), req);
        match svc.await {
            Ok(Either::Right((v, _))) => v,
            e => panic!("{:?}", e),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::convert::Infallible;
    use std::net::TcpListener;

    use http::{Request, Response};
    use hyper::server::conn::AddrStream;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Server};

    use crate::{read_body, TestServer};

    pub async fn svc(listen: TcpListener) -> Result<(), hyper::Error> {
        let make_svc = make_service_fn(|_: &AddrStream| async {
            Ok::<_, Infallible>(service_fn(|_req| async {
                Ok::<_, Infallible>(Response::new(hyper::Body::from("Hello World")))
            }))
        });

        let serv = Server::from_tcp(listen)?.serve(make_svc);
        serv.await?;
        Ok(())
    }

    #[tokio::test]
    async fn make_test() -> Result<(), Box<dyn std::error::Error>> {
        let (listen, ts) = TestServer::new()?;
        let req = Request::get(ts.uri("/")).body(Body::empty())?;
        let request = ts.get(req, svc(listen)).await?;
        let response: String = read_body(request).await?;
        assert_eq!("Hello World", response);
        Ok(())
    }

    #[tokio::test]
    async fn make_post() -> Result<(), Box<dyn std::error::Error>> {
        let (listen, ts) = TestServer::new()?;
        let req = Request::post(ts.uri("/")).body(Body::empty())?;
        let request = ts.get(req, svc(listen)).await?;
        let response = read_body(request).await?;
        assert_eq!("Hello World", response);

        Ok(())
    }
}
