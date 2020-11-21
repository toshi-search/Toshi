use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};
use log::*;
use serde::Deserialize;

use tower_util::BoxService;

use crate::handlers::*;
use crate::settings::Settings;
use crate::utils::{not_found, parse_path};
use toshi_types::Catalog;

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
pub struct Router<C: Catalog> {
    pub cat: Arc<C>,
    pub watcher: Arc<AtomicBool>,
    pub settings: Settings,
}

impl<C: Catalog> Router<C> {
    pub fn new(cat: Arc<C>, watcher: Arc<AtomicBool>) -> Self {
        Self::with_settings(cat, watcher, Settings::default())
    }
    
    pub fn with_settings(cat: Arc<C>, watcher: Arc<AtomicBool>, settings: Settings) -> Self {
        Self { cat, watcher, settings }
    }
    
    pub async fn route(
        catalog: Arc<C>,
        watcher: Arc<AtomicBool>,
        req: Request<Body>,
        settings: Settings,
    ) -> Result<Response<Body>, hyper::Error> {
        let (parts, body) = req.into_parts();
        let query_options: QueryOptions = parts
            .uri
            .query()
            .and_then(|q| serde_urlencoded::from_str(q).ok())
            .unwrap_or_default();

        let method = parts.method;
        let path = parse_path(parts.uri.path());

        match (&method, &path[..]) {
            (m, [idx, "_create"]) if m == Method::PUT => create_index(catalog, body, idx).await,
            (m, [idx, "_summary"]) if m == Method::GET => index_summary(catalog, idx, query_options).await,
            (m, [idx, "_flush"]) if m == Method::GET => flush(catalog, idx).await,
            (m, [idx, "_bulk"]) if m == Method::POST => {
                bulk_insert(catalog, watcher.clone(), body, idx, settings.json_parsing_threads).await
            }
            (m, [idx]) if m == Method::POST => doc_search(catalog, body, idx).await,
            (m, [idx]) if m == Method::PUT => add_document(catalog, body, idx).await,
            (m, [idx]) if m == Method::DELETE => delete_term(catalog, body, idx).await,
            (m, [idx]) if m == Method::GET => {
                if idx == &"favicon.ico" {
                    not_found().await
                } else {
                    all_docs(catalog, idx).await
                }
            }
            (m, []) if m == Method::GET => root::root().await,
            _ => not_found().await,
        }
    }

    pub async fn service_call(catalog: Arc<C>, watcher: Arc<AtomicBool>, settings: Settings) -> Result<BoxedFn, Infallible> {
        Ok(BoxService::new(service_fn(move |req| {
            info!("REQ = {:?}", &req);
            Self::route(Arc::clone(&catalog), Arc::clone(&watcher), req, settings.clone())
        })))
    }

    pub async fn router_with_catalog(self, addr: SocketAddr) -> Result<(), hyper::Error> {
        let routes = make_service_fn(move |_| Self::service_call(Arc::clone(&self.cat), Arc::clone(&self.watcher), self.settings.clone()));
        let server = Server::bind(&addr).serve(routes);
        if let Err(err) = server.await {
            trace!("server error: {}", err);
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn router_from_tcp(self, listener: TcpListener) -> Result<(), hyper::Error> {
        let routes = make_service_fn(move |_| Self::service_call(Arc::clone(&self.cat), Arc::clone(&self.watcher), self.settings.clone()));
        let server = Server::from_tcp(listener)?.serve(routes);
        if let Err(err) = server.await {
            trace!("server error: {}", err);
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use toshi_test::{read_body, TestServer};

    use crate::index::create_test_catalog;
    use crate::router::Router;
    use http::StatusCode;
    use hyper::Body;
    use hyper::Request;

    #[tokio::test]
    async fn test_router() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let router = Router::new(catalog, Arc::new(AtomicBool::new(false)));
        let (listen, ts) = TestServer::new()?;
        let req = Request::get(ts.uri("/")?).body(Body::empty())?;

        let req = ts.get(req, router.router_from_tcp(listen)).await?;
        let resp = read_body(req).await.unwrap();
        assert_eq!(super::root::toshi_info(), resp);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let router = Router::new(catalog, Arc::new(AtomicBool::new(false)));
        let (listen, ts) = TestServer::new()?;
        let req = Request::get(ts.uri("/asdf/asdf")?).body(Body::empty())?;

        let req = ts.get(req, router.router_from_tcp(listen)).await?;
        assert_eq!(req.status(), StatusCode::NOT_FOUND);
        Ok(())
    }
}
