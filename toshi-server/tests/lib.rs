use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use hyper::body::to_bytes;

use toshi::{AsyncClient, HyperToshi};

use toshi_server::index::IndexCatalog;
use toshi_server::router::Router;
use toshi_server::settings::Settings;

type BoxErr = Box<dyn std::error::Error + 'static + Send + Sync>;

#[tokio::test]
async fn test_client() -> Result<(), BoxErr> {
    let addr = "127.0.0.1:8080".parse::<SocketAddr>()?;
    let settings = Settings::default();
    let base = "..\\data".parse::<PathBuf>()?;
    let catalog = IndexCatalog::new(base, settings)?;
    let router = Router::new(Arc::new(catalog), Arc::new(AtomicBool::new(false)));

    tokio::spawn(router.router_with_catalog(addr));

    let client = HyperToshi::new("http://localhost:8080");
    let index = client.index().await?;
    let body = to_bytes(index.into_body()).await?;
    dbg!(body);
    Ok(())
}
