use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use hyper::service::service_fn;
use hyper::{Body, Method, Request, Server};
use serde::Deserialize;
use tokio::prelude::*;

use crate::handlers::*;
use crate::index::IndexCatalog;
use crate::utils::{not_found, parse_path};

#[derive(Deserialize, Debug, Default)]
pub struct QueryOptions {
    pretty: Option<bool>,
    include_sizes: Option<bool>,
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

pub fn router_with_catalog(addr: &SocketAddr, catalog: Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> + Send {
    let routes = move || {
        let search_handler = SearchHandler::new(Arc::clone(&catalog));
        let index_handler = IndexHandler::new(Arc::clone(&catalog));
        let bulk_handler = BulkHandler::new(Arc::clone(&catalog));
        let summary_handler = SummaryHandler::new(Arc::clone(&catalog));

        service_fn(move |req: Request<Body>| {
            let (parts, body) = req.into_parts();

            let query_options: QueryOptions = parts
                .uri
                .query()
                .and_then(|q| serde_urlencoded::from_str(q).ok())
                .unwrap_or_default();

            let method = parts.method;
            let path = parse_path(parts.uri.path());

            log::info!("REQ = {:?}", path);

            match (method, &path[..]) {
                (Method::PUT, [idx, action]) => match *action {
                    "_create" => index_handler.create_index(body, idx.to_string()),
                    _ => not_found(),
                },
                (Method::GET, [idx, action]) => match *action {
                    "_summary" => summary_handler.summary(idx.to_string(), query_options),
                    _ => not_found(),
                },
                (Method::POST, [idx, action]) => match *action {
                    "_bulk" => bulk_handler.bulk_insert(body, idx.to_string()),
                    _ => not_found(),
                },
                (Method::POST, [idx]) => search_handler.doc_search(body, idx.to_string()),
                (Method::PUT, [idx]) => index_handler.add_document(body, idx.to_string()),
                (Method::DELETE, [idx]) => index_handler.delete_term(body, idx.to_string()),
                (Method::GET, [idx]) => {
                    if idx == &"favicon.ico" {
                        not_found()
                    } else {
                        search_handler.all_docs(idx.to_string())
                    }
                }
                (Method::GET, []) => root::root(),
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
        let catalog = create_test_catalog("test_index");
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        router_with_catalog(&addr, Arc::clone(&catalog));
    }
}
