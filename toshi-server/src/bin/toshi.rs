use std::error::Error;
use std::fs::create_dir;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{atomic::AtomicBool, Arc};

use futures::prelude::*;

use log::info;

use tokio::sync::oneshot;

use toshi_raft::rpc_server::RpcServer;
use toshi_server::commit::watcher;
use toshi_server::index::{IndexCatalog, SharedCatalog};
use toshi_server::router::Router;
use toshi_server::settings::{settings, Settings, HEADER};
use toshi_server::{setup_logging_from_file, shutdown};
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

    let index_catalog = setup_catalog(&settings);
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
        tokio::spawn(run_data(Arc::clone(&index_catalog), settings));
        future::try_select(shutdown, master).map(|_| Ok(())).await
    } else {
        let master = run_master(Arc::clone(&index_catalog), settings);
        future::try_select(shutdown, master).map(|_| Ok(())).await
    }
}

fn setup_catalog(settings: &Settings) -> SharedCatalog {
    let path = PathBuf::from(settings.path.clone());
    let index_catalog = match IndexCatalog::new(path, settings.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error creating IndexCatalog from path {} - {:?}", settings.path, e);
            std::process::exit(1);
        }
    };

    info!(
        "Indexes: {:?}",
        index_catalog
            .get_collection()
            .iter()
            .map(|r| r.key().to_owned())
            .collect::<Vec<String>>()
    );
    Arc::new(index_catalog)
}

fn run_data(
    catalog: SharedCatalog,
    settings: Settings,
) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync + 'static>>> + Unpin + Send {
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid IP address: {}", &settings.host));

    let bind: SocketAddr = SocketAddr::new(addr, settings.experimental_features.rpc_port);
    let fut = async move {
        if let Err(e) = RpcServer::<IndexCatalog>::serve(bind, catalog, slog_scope::logger()).await {
            eprintln!("ERROR = {:?}", e);
        }
        Ok(())
    };
    Box::pin(fut)
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
    let router = Router::new(catalog, watcher_clone);
    Box::pin(router.router_with_catalog(bind))
}
