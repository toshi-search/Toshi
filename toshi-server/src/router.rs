use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};

use log::*;
use tower_util::BoxService;

use toshi_types::{Catalog, QueryOptions, Serve};

use crate::handlers::*;
use crate::local_serve::LocalServe;
use crate::settings::Settings;
use crate::utils::{not_found, parse_path};

pub type BoxedFn = BoxService<Request<Body>, Response<Body>, hyper::Error>;

#[derive(Clone)]
pub struct Router<C: Catalog> {
    pub cat: Arc<C>,
    pub watcher: Arc<AtomicBool>,
    pub settings: Settings,
}

impl<C: Catalog> Router<C> {
    pub fn new(cat: Arc<C>, watcher: Arc<AtomicBool>) -> Self {
        Self::from_settings(cat, watcher, Settings::default())
    }

    pub fn from_settings(cat: Arc<C>, watcher: Arc<AtomicBool>, settings: Settings) -> Self {
        Self { cat, watcher, settings }
    }

    fn make_serve() -> impl Serve<C> {
        LocalServe
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

        let serve = Self::make_serve();

        match (&method, &path[..]) {
            (m, ["_list"]) if m == Method::GET => serve.list_indexes(catalog).await,
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
            (m, []) if m == Method::GET => root().await,
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
