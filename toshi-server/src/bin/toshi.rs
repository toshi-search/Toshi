use std::error::Error;
use std::fs::create_dir;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::{atomic::AtomicBool, Arc};

use futures::prelude::*;

use log::info;

use tokio::sync::oneshot;

use std::str::FromStr;
use toshi_server::commit::watcher;
use toshi_server::index::IndexCatalog;
use toshi_server::router::Router;
use toshi_server::settings::{settings, Settings, HEADER};
use toshi_server::{setup_logging_from_file, shutdown, SharedCatalog};
use toshi_types::Catalog;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let settings = settings();
    let logger = setup_logging_from_file("config/logging.toml")?;
    let _scope = slog_scope::set_global_logger(logger.clone());
    let _guard = slog_stdlog::init_with_level(log::Level::from_str(&settings.log_level)?)?;

    let (tx, shutdown_signal) = oneshot::channel();
    if !Path::new(&settings.path).exists() {
        info!("Base data path {} does not exist, creating it...", settings.path);
        create_dir(settings.path.clone()).expect("Unable to create data directory");
    }

    let index_catalog = setup_catalog(&settings).await?;

    let s_clone = settings.clone();
    let toshi = setup_toshi(s_clone.clone(), Arc::clone(&index_catalog), tx);
    tokio::spawn(toshi);
    info!("Toshi running on {}:{}", &settings.host, &settings.port);

    setup_shutdown(shutdown_signal, index_catalog).await.map_err(Into::into)
}

async fn setup_shutdown(shutdown_signal: oneshot::Receiver<()>, index_catalog: SharedCatalog) -> Result<(), oneshot::error::RecvError> {
    shutdown_signal.await?;
    info!("Shutting down...");
    index_catalog.clear().await;
    Ok(())
}

async fn setup_toshi(settings: Settings, index_catalog: SharedCatalog, tx: oneshot::Sender<()>) -> Result<(), ()> {
    let shutdown = shutdown::shutdown(tx);
    if settings.experimental {
        let master = run_master(Arc::clone(&index_catalog), settings.clone());
        future::try_select(shutdown, master).map(|_| Ok(())).await
    } else {
        let master = run_master(Arc::clone(&index_catalog), settings);
        future::try_select(shutdown, master).map(|_| Ok(())).await
    }
}

async fn setup_catalog(settings: &Settings) -> Result<SharedCatalog, toshi_types::Error> {
    let mut index_catalog = match IndexCatalog::new(settings.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error creating IndexCatalog from path {} - {:?}", settings.path, e);
            std::process::exit(1);
        }
    };
    index_catalog.refresh_catalog().await?;
    info!("{} Indexes loaded...", index_catalog.get_collection().len());
    Ok(Arc::new(index_catalog))
}

fn run_master(catalog: SharedCatalog, settings: Settings) -> impl Future<Output = Result<(), hyper::Error>> + Unpin + Send {
    let bulk_lock = Arc::new(AtomicBool::new(false));
    let commit_watcher = watcher(Arc::clone(&catalog), settings.auto_commit_duration, Arc::clone(&bulk_lock));
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid ip address: {}", &settings.host));
    let bind: SocketAddr = SocketAddr::new(addr, settings.port);

    println!("{}", HEADER);

    tokio::spawn(commit_watcher);
    let watcher_clone = Arc::clone(&bulk_lock);
    let router = Router::from_settings(catalog, watcher_clone, settings);
    Box::pin(router.router_with_catalog(bind))
}
