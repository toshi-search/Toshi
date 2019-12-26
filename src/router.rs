use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use hyper::{Body, Method, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use serde::Deserialize;
use tower_util::BoxService;
use tracing::info;

use crate::handlers::*;
use crate::index::SharedCatalog;
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

pub type BoxedFn = BoxService<Request<Body>, Response<Body>, hyper::Error>;

#[derive(Clone)]
pub struct Router {
    pub cat: SharedCatalog,
    pub watcher: Arc<AtomicBool>,
}

impl Router {
    pub fn new(cat: SharedCatalog, watcher: Arc<AtomicBool>) -> Self {
        Self { cat, watcher }
    }

    pub async fn route(catalog: SharedCatalog, watcher: Arc<AtomicBool>, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let (parts, body) = req.into_parts();
        let query_options: QueryOptions = parts
            .uri
            .query()
            .and_then(|q| serde_urlencoded::from_str(q).ok())
            .unwrap_or_default();

        let method = parts.method;
        let path = parse_path(parts.uri.path());

        match (&method, &path[..]) {
            (m, [idx, "_create"]) if m == Method::PUT => create_index(catalog, body, (*idx).to_string()).await,
            (m, [idx, "_summary"]) if m == Method::GET => index_summary(catalog, (*idx).to_string(), query_options).await,
            (m, [idx, "_flush"]) if m == Method::GET => flush(catalog, (*idx).to_string()).await,
            (m, [idx, "_bulk"]) if m == Method::POST => bulk_insert(catalog, watcher.clone(), body, (*idx).to_string()).await,
            (m, [idx]) if m == Method::POST => doc_search(catalog, body, (*idx).to_string()).await,
            (m, [idx]) if m == Method::PUT => add_document(catalog, body, (*idx).to_string()).await,
            (m, [idx]) if m == Method::DELETE => delete_term(catalog, body, (*idx).to_string()).await,
            (m, [idx]) if m == Method::GET => {
                if idx == &"favicon.ico" {
                    not_found().await
                } else {
                    all_docs(catalog, (*idx).to_string()).await
                }
            }
            (m, []) if m == Method::GET => root::root().await,
            _ => not_found().await,
        }
    }

    pub async fn service_call(catalog: SharedCatalog, watcher: Arc<AtomicBool>) -> Result<BoxedFn, Infallible> {
        Ok(BoxService::new(service_fn(move |req| {
            info!("REQ = {:?}", &req);
            Self::route(Arc::clone(&catalog), Arc::clone(&watcher), req)
        })))
    }

    pub async fn router_with_catalog(self, addr: SocketAddr) -> Result<(), hyper::Error> {
        let routes = make_service_fn(move |_| Self::service_call(Arc::clone(&self.cat), Arc::clone(&self.watcher)));
        let server = Server::bind(&addr).serve(routes);
        if let Err(err) = server.await {
            tracing::error!("server error: {}", err);
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use toshi_test::TestServer;

    use crate::router::Router;

    #[tokio::test]
    async fn test_router() {
        let catalog = crate::index::tests::create_test_catalog("test_index");
        let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
        let router = Router::new(catalog, Arc::new(AtomicBool::new(false)));
        let mut ts = TestServer::with_server(router.router_with_catalog(addr)).unwrap();

        let req = ts.get("/").await.unwrap();
        let resp = TestServer::read_body(req).await.unwrap();
        assert_eq!(super::root::toshi_info(), resp);
    }
}
