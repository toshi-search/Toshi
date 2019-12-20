use std::fs::create_dir;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};

use futures::prelude::*;

use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tracing::*;

use toshi_server::commit::watcher;
use toshi_server::index::{IndexCatalog, SharedCatalog};
use toshi_server::router::Router;
use toshi_server::settings::{Settings, HEADER, RPC_HEADER};
use toshi_server::{shutdown, support};

#[cfg_attr(tarpaulin, skip)]
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = support::settings();
    setup_logging(&settings.log_level);

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

#[cfg_attr(tarpaulin, skip)]
fn setup_logging(level: &str) {
    std::env::set_var("RUST_LOG", level);
    let sub = tracing_fmt::FmtSubscriber::builder().with_ansi(true).finish();
    tracing::subscriber::set_global_default(sub).expect("Unable to set default Subscriber");
}

#[cfg_attr(tarpaulin, skip)]
async fn setup_shutdown(shutdown_signal: Receiver<()>, index_catalog: SharedCatalog) -> Result<(), oneshot::error::RecvError> {
    shutdown_signal.await?;
    info!("Shutting down...");
    index_catalog.lock().await.clear().await;
    Ok(())
}

#[cfg_attr(tarpaulin, skip)]
async fn setup_toshi(settings: Settings, index_catalog: SharedCatalog, tx: Sender<()>) -> Result<(), ()> {
    let shutdown = shutdown::shutdown(tx);
    //    if !settings.experimental_features.master && settings.experimental {
    //        select(run_data(Arc::clone(&index_catalog), settings), shutdown).then(|_| {
    //            Ok(())
    //        }).await
    //    } else {
    let master = run_master(Arc::clone(&index_catalog), settings);
    future::try_select(shutdown, master).map(|_| Ok(())).await
    //    }
}

#[cfg_attr(tarpaulin, skip)]
fn setup_catalog(settings: &Settings) -> SharedCatalog {
    let path = PathBuf::from(settings.path.clone());
    let index_catalog = match IndexCatalog::new(path, settings.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error creating IndexCatalog from path {} - {}", settings.path, e);
            std::process::exit(1);
        }
    };
    info!("Indexes: {:?}", index_catalog.get_collection().keys());
    Arc::new(Mutex::new(index_catalog))
}

#[cfg_attr(tarpaulin, skip)]
async fn run_data(catalog: Arc<Mutex<IndexCatalog>>, settings: &Settings) -> Result<(), hyper::Error> {
    let lock = Arc::new(AtomicBool::new(false));
    let commit_watcher = watcher(Arc::clone(&catalog), settings.auto_commit_duration, Arc::clone(&lock));
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid ip address: {}", &settings.host));
    let settings = settings.clone();
    let bind: SocketAddr = SocketAddr::new(addr, settings.port);

    println!("{}", RPC_HEADER);
    info!("I am a data node...Binding to: {}", addr);
    Ok(())
}

#[cfg_attr(tarpaulin, skip)]
fn run_master(catalog: Arc<Mutex<IndexCatalog>>, settings: Settings) -> impl Future<Output = Result<(), hyper::Error>> + Unpin + Send {
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
    Box::pin(router.router_with_catalog(bind.clone()))
}
