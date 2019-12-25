use std::error::Error;
use std::net::SocketAddr;

use futures::Future;
use http::{Request, Response};
use hyper::client::ResponseFuture;
use hyper::server::conn::AddrStream;
use hyper::service::Service;
use hyper::{Body, Server};
use tantivy::schema::*;
use tantivy::{doc, Index};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

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
    rt: Handle,
}

impl TestServer {
    pub fn new<Fut, RetFut, Svc, Err, MkSvc>(h: Handle, svc: Svc) -> Result<Self, Err>
    where
        Fut: Future<Output = Result<MkSvc, Err>> + Send + 'static,
        for<'a> Svc: Service<&'a AddrStream, Future = Fut, Error = Err, Response = MkSvc> + Send + 'static,
        RetFut: Future<Output = Result<Response<Body>, Err>> + Send + 'static,
        MkSvc: Service<Request<Body>, Future = RetFut, Error = Err, Response = Response<Body>> + Send + 'static,
        Err: Into<Box<dyn Error + Send + Sync>> + 'static,
    {
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        let server = Server::bind(&addr).serve(svc);
        h.spawn(server);
        Ok(TestServer { rt: h })
    }

    pub fn run(&mut self, req: ResponseFuture) -> JoinHandle<Result<Response<Body>, hyper::Error>> {
        //        let res = tokio::sync::oneshot::Oneshot::new();
        self.rt.spawn(req)
    }
}

#[cfg(test)]
pub mod tests {
    use std::convert::Infallible;

    use http::{Response, Uri};
    use hyper::server::conn::AddrStream;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Client, Error};
    use tokio::runtime::{Builder, Runtime};

    use crate::TestServer;

    //    #[tokio::test(threaded_scheduler)]
    #[test]
    fn make_test() {
        let rt = Runtime::new().unwrap();
        let h = rt.handle();
        rt.enter(move || {
            let make_svc = make_service_fn(|_: &AddrStream| {
                async {
                    Ok::<_, Infallible>(service_fn(|_req| {
                        async { Ok::<_, Infallible>(Response::new(hyper::Body::from("Hello World"))) }
                    }))
                }
            });

            let mut ts = TestServer::new((*h).clone(), make_svc).unwrap();

            let client = Client::new();
            let addr = "127.0.0.1:8080".parse::<Uri>().unwrap();

            let req = client.get(addr);
            let resp = ts.run(req);
            println!("{:?}", resp);
        });
    }
}
