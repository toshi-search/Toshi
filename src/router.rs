use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};
use serde::Deserialize;
use tower_util::BoxService;

use crate::handlers::*;
use crate::index::SharedCatalog;
use crate::utils::{not_found, parse_path};
use std::convert::Infallible;

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
        tracing::info!("REQ = {:?}", path);

        match (&method, &path[..]) {
            (m, [idx, action]) if m == Method::PUT => match *action {
                "_create" => create_index(catalog.clone(), body, (*idx).to_string()).await,
                _ => not_found().await,
            },
            (m, [idx, action]) if m == Method::GET => match *action {
                "_summary" => index_summary(catalog.clone(), (*idx).to_string(), query_options).await,
                "_flush" => flush(catalog.clone(), (*idx).to_string()).await,
                _ => not_found().await,
            },
            (m, [idx, action]) if m == Method::POST => match *action {
                "_bulk" => bulk_insert(catalog.clone(), watcher.clone(), body, (*idx).to_string()).await,
                _ => not_found().await,
            },
            (m, [idx]) if m == Method::POST => doc_search(catalog.clone(), body, (*idx).to_string()).await,
            (m, [idx]) if m == Method::PUT => add_document(catalog.clone(), body, (*idx).to_string()).await,
            (m, [idx]) if m == Method::DELETE => delete_term(catalog.clone(), body, (*idx).to_string()).await,
            (m, [idx]) if m == Method::GET => {
                if idx == &"favicon.ico" {
                    not_found().await
                } else {
                    all_docs(catalog.clone(), (*idx).to_string()).await
                }
            }
            (m, []) if m == Method::GET => root::root().await,
            _ => not_found().await,
        }
    }

    pub async fn service_call(catalog: SharedCatalog, watcher: Arc<AtomicBool>) -> Result<BoxedFn, Infallible> {
        Ok(BoxService::new(service_fn(move |req| {
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
