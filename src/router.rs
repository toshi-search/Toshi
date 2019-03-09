use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use flate2::Compression;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tower_web::middleware::deflate::DeflateMiddleware;
use tower_web::middleware::log::LogMiddleware;
use tower_web::ServiceBuilder;

use crate::handlers::*;
use crate::index::IndexCatalog;
use crate::settings::VERSION;

pub fn router_with_catalog(addr: &SocketAddr, catalog: &Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> + Send {
    let search_handler = SearchHandler::new(Arc::clone(catalog));
    let index_handler = IndexHandler::new(Arc::clone(catalog));
    let bulk_handler = BulkHandler::new(Arc::clone(catalog));
    let summary_handler = SummaryHandler::new(Arc::clone(catalog));
    let root_handler = RootHandler::new(VERSION);
    let listener = TcpListener::bind(addr).unwrap().incoming();

    ServiceBuilder::new()
        .resource(search_handler)
        .resource(index_handler)
        .resource(bulk_handler)
        .resource(summary_handler)
        .resource(root_handler)
        .middleware(LogMiddleware::new("toshi::router"))
        .middleware(DeflateMiddleware::new(Compression::fast()))
        .serve(listener)
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::index::tests::create_test_catalog;

    #[test]
    pub fn test_create_router() {
        let catalog = create_test_catalog("test_index");
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        router_with_catalog(&addr, &catalog);
    }
}
