use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use hyper::service::service_fn;
use hyper::{Body, Method, Request, Server};
use tokio::prelude::*;

use crate::handlers::*;
use crate::index::IndexCatalog;
use crate::settings::VERSION;
use http::{Response, StatusCode};

fn not_found() -> ResponseFuture {
    let not_found = Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap();
    Box::new(future::ok(not_found))
}

fn parse_path(path: &str) -> Vec<&str> {
    path.split('/').collect::<Vec<_>>()
}

pub fn router_with_catalog(addr: &SocketAddr, catalog: Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> + Send {
    let r = move || {
        let search_handler = SearchHandler::new(Arc::clone(&catalog));
        let _index_handler = IndexHandler::new(Arc::clone(&catalog));
        let _bulk_handler = BulkHandler::new(Arc::clone(&catalog));
        let _summary_handler = SummaryHandler::new(Arc::clone(&catalog));
        let root = RootHandler::new(VERSION);

        service_fn(move |req: Request<Body>| {
            let path = parse_path(req.uri().path());
            log::info!("PATH = {:?}", &path);
            match (req.method(), &path[..]) {
                (&Method::GET, [_, idx, action]) => search_handler.get_all_docs(idx.to_string()),
                (&Method::GET, [_, idx]) if !idx.is_empty() => search_handler.get_all_docs(idx.to_string()),
                (&Method::GET, [_, idx]) if idx.is_empty() => root.call(),
                _ => not_found(),
            }
        })
    };

    Server::bind(addr)
        .tcp_nodelay(true)
        .http1_half_close(false)
        .serve(r)
        .map_err(|e| eprintln!("HYPER ERROR = {:?}", e))

    //    ServiceBuilder::new()
    //        .resource(search_handler)
    //        .resource(index_handler)
    //        .resource(bulk_handler)
    //        .resource(summary_handler)
    //        .resource(root_handler)
    //        .middleware(LogMiddleware::new("toshi::router"))
    //        .middleware(DeflateMiddleware::new(Compression::fast()))
    //        .serve(listener)
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
