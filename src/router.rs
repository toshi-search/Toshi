use std::net::SocketAddr;
use std::sync::Arc;

use hyper::service::service_fn;
use hyper::{Body, Method, Request, Server};
use parking_lot::RwLock;
use serde::Deserialize;
use tokio::prelude::*;

use crate::commit::IndexWatcher;
use crate::handlers::*;
use crate::index::IndexCatalog;
use crate::utils::{not_found, parse_path};

#[derive(Deserialize, Debug, Default)]
pub struct QueryOptions {
    pub pretty: Option<bool>,
    pub include_sizes: Option<bool>,
}

impl QueryOptions {
    #[inline]
    pub fn include_sizes(&self) -> bool {
        self.include_sizes.unwrap_or(false)
    }

    #[inline]
    pub fn pretty(&self) -> bool {
        self.pretty.unwrap_or(false)
    }
}

pub fn router_with_catalog(
    addr: &SocketAddr,
    catalog: Arc<RwLock<IndexCatalog>>,
    watcher: Arc<IndexWatcher>,
) -> impl Future<Item = (), Error = ()> + Send {
    let routes = move || {
        let search_handler = SearchHandler::new(Arc::clone(&catalog));
        let index_handler = IndexHandler::new(Arc::clone(&catalog));
        let bulk_handler = BulkHandler::new(Arc::clone(&catalog), Arc::clone(&watcher));
        let summary_cat = Arc::clone(&catalog);

        service_fn(move |req: Request<Body>| {
            let summary_cat = &summary_cat;

            let (parts, body) = req.into_parts();

            let query_options: QueryOptions = parts
                .uri
                .query()
                .and_then(|q| serde_urlencoded::from_str(q).ok())
                .unwrap_or_default();

            let method = parts.method;
            let path = parse_path(parts.uri.path());

            tracing::info!("REQ = {:?}", path);

            match (&method, &path[..]) {
                (m, [idx, action]) if m == Method::PUT => match *action {
                    "_create" => index_handler.create_index(body, idx.to_string()),
                    _ => not_found(),
                },
                (m, [idx, action]) if m == Method::GET => match *action {
                    "_summary" => summary(Arc::clone(summary_cat), idx.to_string(), query_options),
                    _ => not_found(),
                },
                (m, [idx, action]) if m == Method::POST => match *action {
                    "_bulk" => bulk_handler.bulk_insert(body, idx.to_string()),
                    _ => not_found(),
                },
                (m, [idx]) if m == Method::POST => search_handler.doc_search(body, idx.to_string()),
                (m, [idx]) if m == Method::PUT => index_handler.add_document(body, idx.to_string()),
                (m, [idx]) if m == Method::DELETE => index_handler.delete_term(body, idx.to_string()),
                (m, [idx]) if m == Method::GET => {
                    if idx == &"favicon.ico" {
                        not_found()
                    } else {
                        search_handler.all_docs(idx.to_string())
                    }
                }
                (m, []) if m == Method::GET => root::root(),
                _ => not_found(),
            }
        })
    };

    Server::bind(addr)
        .tcp_nodelay(true)
        .http1_half_close(false)
        .serve(routes)
        .map_err(|e| tracing::error!("HYPER ERROR = {:?}", e))
}

#[cfg(test)]
pub mod tests {
    use crate::index::tests::create_test_catalog;

    use super::*;
    use http::StatusCode;
    use lazy_static::lazy_static;
    use toshi_test::{get_localhost, TestServer};

    lazy_static! {
        pub static ref TEST_SERVER: TestServer = {
            let catalog = create_test_catalog("test_index");
            let addr = get_localhost();
            let watcher = Arc::new(IndexWatcher::new(Arc::clone(&catalog), 100));
            let router = router_with_catalog(&addr, Arc::clone(&catalog), Arc::clone(&watcher));
            TestServer::new(router).expect("Can't start test server")
        };
    }

    #[test]
    pub fn test_create_router() {
        let addr = get_localhost();
        let response = TEST_SERVER
            .client_with_address(addr)
            .get("http://localhost:8080")
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let buf = String::from_utf8(response.into_body().concat2().wait().unwrap().to_vec()).unwrap();
        assert_eq!(buf, "{\"name\":\"Toshi Search\",\"version\":\"0.1.1\"}");
    }
}
