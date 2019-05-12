use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use http::{Response, StatusCode};
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Server};
use tokio::prelude::*;

use crate::handlers::*;
use crate::index::IndexCatalog;

pub fn empty_with_code(code: StatusCode) -> http::Response<Body> {
    Response::builder().status(code).body(Body::empty()).unwrap()
}

fn not_found() -> ResponseFuture {
    let not_found = empty_with_code(StatusCode::NOT_FOUND);
    Box::new(future::ok(not_found))
}

fn parse_path(path: &str) -> Vec<&str> {
    path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect::<Vec<_>>()
}

pub fn router_with_catalog(addr: &SocketAddr, catalog: Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> + Send {
    let routes = move || {
        let search_handler = SearchHandler::new(Arc::clone(&catalog));
        let index_handler = IndexHandler::new(Arc::clone(&catalog));
        let bulk_handler = BulkHandler::new(Arc::clone(&catalog));
        let summary_handler = SummaryHandler::new(Arc::clone(&catalog));

        service_fn(move |req: Request<Body>| {
            let raw_path = req.uri().clone();
            let path = parse_path(raw_path.path());

            log::info!("PATH = {:?}", &path);
            match (req.method(), &path[..]) {
                (&Method::PUT, [idx, action]) => match *action {
                    "_create" => index_handler.create_index(req.into_body(), idx.to_string()),
                    "" => index_handler.add_document(req.into_body(), idx.to_string()),
                    _ => not_found(),
                },
                (&Method::GET, [idx, action]) => match *action {
                    "_summary" => summary_handler.summary(idx.to_string()),
                    _ => not_found(),
                },
                (&Method::POST, [idx, action]) => match *action {
                    "_bulk" => bulk_handler.bulk_insert(req.into_body(), idx.to_string()),
                    "" => search_handler.all_docs(idx.to_string()),
                    _ => not_found(),
                },
                (&Method::POST, [idx]) => search_handler.doc_search(req.into_body(), idx.to_string()),
                (&Method::GET, [idx]) => {
                    if idx == &"favicon.ico" {
                        not_found()
                    } else {
                        search_handler.all_docs(idx.to_string())
                    }
                }
                (&Method::GET, []) => root::root(),
                _ => not_found(),
            }
        })
    };

    Server::bind(addr)
        .tcp_nodelay(true)
        .http1_half_close(false)
        .serve(routes)
        .map_err(|e| eprintln!("HYPER ERROR = {:?}", e))
}

#[cfg(test)]
pub mod tests {
    use crate::index::tests::create_test_catalog;

    use super::*;

    #[test]
    pub fn test_create_router() {
        std::env::set_var("RUST_LOG", "info");
        pretty_env_logger::init();
        let catalog = create_test_catalog("test_index");
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        hyper::rt::run(router_with_catalog(&addr, Arc::clone(&catalog)));
    }
}
