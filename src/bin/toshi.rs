use std::error::Error;
use std::fs::create_dir;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures::{future, Future};
use parking_lot::RwLock;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::*;

use toshi::cluster::rpc_server::RpcServer;
use toshi::commit::watcher;
use toshi::index::IndexCatalog;
use toshi::router::router_with_catalog;
use toshi::settings::{Settings, HEADER, RPC_HEADER};
use toshi::{shutdown, support};

fn get_subscriber() -> impl Subscriber {
    tracing_fmt::FmtSubscriber::builder()
        .with_ansi(true)
        .finish()
}

//noinspection ALL
pub fn main() -> Result<(), ()> {
    let settings = support::settings();

    std::env::set_var("RUST_LOG", &settings.log_level);
    let sub = get_subscriber();
    tracing::subscriber::set_global_default(sub).expect("Unable to set default Subscriber");

    debug!("{:?}", &settings);

    let mut rt = Runtime::new().expect("failed to start new Runtime");

    let (tx, shutdown_signal) = oneshot::channel();

    if !Path::new(&settings.path).exists() {
        info!("Base data path {} does not exist, creating it...", settings.path);
        create_dir(settings.path.clone()).expect("Unable to create data directory");
    }

    let index_catalog = {
        let path = PathBuf::from(settings.path.clone());
        let index_catalog = match IndexCatalog::new(path, settings.clone()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Error creating IndexCatalog from path {} - {}", settings.path, e);
                std::process::exit(1);
            }
        };

        Arc::new(RwLock::new(index_catalog))
    };

    let toshi = {
        let server = if !settings.experimental_features.master && settings.experimental {
            future::Either::A(run_data(Arc::clone(&index_catalog), &settings))
        } else {
            future::Either::B(run_master(Arc::clone(&index_catalog), &settings))
        };
        let shutdown = shutdown::shutdown(tx);
        server.select(shutdown)
    };

    rt.spawn(toshi.map(|_| ()).map_err(|_| ()));
    info!("Toshi running on {}:{}", &settings.host, &settings.port);

    shutdown_signal
        .map_err(|e| unreachable!("Shutdown signal channel should not error, This is a bug. \n {:?} ", e.description()))
        .and_then(move |_| {
            index_catalog.write().clear();
            Ok(())
        })
        .and_then(move |_| rt.shutdown_now())
        .wait()
}

fn run_data(catalog: Arc<RwLock<IndexCatalog>>, settings: &Settings) -> impl Future<Item = (), Error = ()> {
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
    commit_watcher.and_then(move |_| RpcServer::serve(bind, catalog))
}

fn run_master(catalog: Arc<RwLock<IndexCatalog>>, settings: &Settings) -> impl Future<Item = (), Error = ()> {
    let bulk_lock = Arc::new(AtomicBool::new(false));
    let commit_watcher = watcher(Arc::clone(&catalog), settings.auto_commit_duration, Arc::clone(&bulk_lock));
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid ip address: {}", &settings.host));
    let bind: SocketAddr = SocketAddr::new(addr, settings.port);

    println!("{}", HEADER);

    if settings.experimental {
        let settings = settings.clone();
        let nodes = settings.experimental_features.nodes.clone();

        let run = commit_watcher.and_then(move |_| {
            if !nodes.is_empty() {
                let update = catalog.read().update_remote_indexes();
                tokio::spawn(update);
            }

            router_with_catalog(&bind, Arc::clone(&catalog), Arc::clone(&bulk_lock))
        });
        future::Either::A(run)
    } else {
        let watcher_clone = Arc::clone(&bulk_lock);
        let run = future::lazy(move || {
            tokio::spawn(commit_watcher);
            router_with_catalog(&bind, Arc::clone(&catalog), watcher_clone)
        });
        future::Either::B(run)
    }
}
