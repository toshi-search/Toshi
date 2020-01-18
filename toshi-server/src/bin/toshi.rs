use std::fs::create_dir;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};

use futures::prelude::*;
use slog::Drain;
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::Mutex;
use toshi_raft::rpc_server::RpcServer;
use toshi_server::commit::watcher;
use toshi_server::index::{IndexCatalog, SharedCatalog};
use toshi_server::router::Router;
use toshi_server::settings::{Settings, HEADER, RPC_HEADER};
use toshi_server::{shutdown, support};
use toshi_types::Catalog;
use tracing::*;

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
    if settings.experimental && settings.experimental_features.master {
        let update_cat = Arc::clone(&index_catalog);
        tokio::spawn(async move {
            let update = update_cat.lock().await;
            update.update_remote_indexes().await
        });
    }
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
    if !settings.experimental_features.master && settings.experimental {
        let data = run_data(Arc::clone(&index_catalog), settings);
        future::try_select(shutdown, data).map(|_| Ok(())).await
    } else {
        let master = run_master(Arc::clone(&index_catalog), settings);
        future::try_select(shutdown, master).map(|_| Ok(())).await
    }
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
fn run_data(
    catalog: Arc<Mutex<IndexCatalog>>,
    settings: Settings,
) -> impl Future<Output = Result<(), tonic::transport::Error>> + Unpin + Send {
    let lock = Arc::new(AtomicBool::new(false));
    let commit_watcher = watcher(Arc::clone(&catalog), settings.auto_commit_duration, Arc::clone(&lock));
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid IP address: {}", &settings.host));

    let bind: SocketAddr = SocketAddr::new(addr, settings.port);
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).use_local_timestamp().build().fuse();
    let async_drain = slog_async::Async::new(drain).build().fuse();
    let root_log = slog::Logger::root(async_drain, slog::o!("toshi" => "toshi"));

    println!("{}", RPC_HEADER);
    info!("I am a data node...Binding to: {}", addr);
    tokio::spawn(commit_watcher);
    Box::pin(RpcServer::<IndexCatalog>::serve(bind, catalog, root_log))
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
    Box::pin(router.router_with_catalog(bind))
}
