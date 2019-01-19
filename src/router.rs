use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use flate2::Compression;
use http::response::Builder;
use http::Request;
use log::info;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tower_web::middleware::deflate::DeflateMiddleware;
use tower_web::middleware::log::LogMiddleware;
use tower_web::Error as TowerError;
use tower_web::ServiceBuilder;

use crate::handlers::*;
use crate::index::IndexCatalog;
use crate::settings::VERSION;

pub fn router_with_catalog(addr: &SocketAddr, catalog: &Arc<RwLock<IndexCatalog>>) -> Box<Future<Item = (), Error = ()> + Send> {
    let search_handler = SearchHandler::new(Arc::clone(catalog));
    let index_handler = IndexHandler::new(Arc::clone(catalog));
    let bulk_handler = BulkHandler::new(Arc::clone(catalog));
    let summary_handler = SummaryHandler::new(Arc::clone(catalog));
    let root_handler = RootHandler::new(VERSION);
    let listener = TcpListener::bind(addr).unwrap().incoming();

    let router = ServiceBuilder::new()
        .resource(search_handler)
        .resource(index_handler)
        .resource(bulk_handler)
        .resource(summary_handler)
        .resource(root_handler)
        .middleware(LogMiddleware::new("toshi"))
        .middleware(DeflateMiddleware::new(Compression::fast()))
        .catch(|request: &Request<()>, error: TowerError| {
            info!("{:?}", error);
            let err_msg = ErrorResponse::new(error.to_string(), request.uri().path().into());
            let json = serde_json::to_string(&err_msg).unwrap();

            let (status, body) = match error.kind() {
                e if e.is_not_found() => (404, json),
                e if e.is_bad_request() => (400, json),
                e if e.is_internal() => (500, json),
                _ => (404, json),
            };
            let response = Builder::new()
                .header("content-type", "application/json")
                .status(status)
                .body(body)
                .unwrap();

            Ok(response)
        })
        .serve(listener);

    Box::new(router)
}
