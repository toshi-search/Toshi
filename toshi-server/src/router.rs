use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;

use log::*;

use toshi_types::{Body, Catalog, QueryOptions};

use crate::handlers::*;
use crate::settings::Settings;
use crate::utils::{not_found, parse_path};

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

    pub async fn route(
        catalog: Arc<C>,
        watcher: Arc<AtomicBool>,
        req: Request<Incoming>,
        settings: Settings,
    ) -> Result<Response<Body>, hyper::Error> {
        let (parts, incoming) = req.into_parts();
        // hyper 1.x delivers request bodies as a streaming `Incoming`. The handlers operate on a
        // fully-buffered `Full<Bytes>`, so collect the request body once here before dispatching.
        let body: Body = Body::new(incoming.collect().await?.to_bytes());
        let query_options: QueryOptions = parts
            .uri
            .query()
            .and_then(|q| serde_urlencoded::from_str(q).ok())
            .unwrap_or_default();

        let method = parts.method;
        let path = parse_path(parts.uri.path());

        match (&method, &path[..]) {
            (m, ["_list"]) if m == Method::GET => list_indexes(catalog).await,
            (m, [idx, "_create"]) if m == Method::PUT => create_index(catalog, body, idx).await,
            (m, [idx, "_summary"]) if m == Method::GET => index_summary(catalog, idx, query_options).await,
            (m, [idx, "_flush"]) if m == Method::GET => flush(catalog, idx).await,
            (m, [idx, "_bulk"]) if m == Method::POST => {
                let w = Arc::clone(&watcher);
                bulk_insert(catalog, w, body, idx, settings.json_parsing_threads, settings.max_line_length).await
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

    pub async fn router_with_catalog(self, addr: SocketAddr) -> Result<(), hyper::Error> {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(err) => {
                trace!("server error: {}", err);
                return Ok(());
            }
        };
        self.serve(listener).await
    }

    #[allow(dead_code)]
    pub(crate) async fn router_from_tcp(self, listener: std::net::TcpListener) -> Result<(), hyper::Error> {
        listener.set_nonblocking(true).expect("Unable to set listener to non-blocking");
        let listener = TcpListener::from_std(listener).expect("Unable to convert to tokio TcpListener");
        self.serve(listener).await
    }

    /// Accept connections on `listener`, serving each on its own task with the hyper-util
    /// auto (HTTP/1 + HTTP/2) connection builder. In hyper 1.x the high-level `Server` was
    /// removed, so the accept loop is now explicit.
    async fn serve(self, listener: TcpListener) -> Result<(), hyper::Error> {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(err) => {
                    trace!("accept error: {}", err);
                    continue;
                }
            };
            let io = TokioIo::new(stream);
            let cat = Arc::clone(&self.cat);
            let watcher = Arc::clone(&self.watcher);
            let settings = self.settings.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    info!("REQ = {:?}", &req);
                    Self::route(Arc::clone(&cat), Arc::clone(&watcher), req, settings.clone())
                });
                if let Err(err) = auto::Builder::new(TokioExecutor::new()).serve_connection(io, service).await {
                    trace!("server error: {}", err);
                }
            });
        }
    }
}
