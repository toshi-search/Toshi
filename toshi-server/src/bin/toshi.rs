use std::fs::create_dir;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};

use dashmap::DashMap;
use futures::prelude::*;
use http::Uri;
use raft::Config;
use tokio::sync::oneshot::{self, Receiver, Sender};
use tonic::Request;
use tracing::*;

use toshi_raft::raft_node::ToshiRaft;
use toshi_raft::rpc_server::{create_client, RpcClient, RpcServer};
use toshi_server::commit::watcher;
use toshi_server::index::{IndexCatalog, SharedCatalog};
use toshi_server::router::Router;
use toshi_server::settings::{settings, Experimental, Settings, HEADER, RPC_HEADER};
use toshi_server::shutdown;
use toshi_types::Catalog;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = settings();
    setup_logging(&settings.log_level);

    let (tx, shutdown_signal) = oneshot::channel();
    if !Path::new(&settings.path).exists() {
        info!("Base data path {} does not exist, creating it...", settings.path);
        create_dir(settings.path.clone()).expect("Unable to create data directory");
    }

    let index_catalog = setup_catalog(&settings);
    let s_clone = settings.clone();
    let toshi = setup_toshi(s_clone.clone(), Arc::clone(&index_catalog), tx);
    if settings.experimental && settings.experimental_features.leader {
        let update_cat = Arc::clone(&index_catalog);
        tokio::spawn(async move { update_cat.update_remote_indexes().await });
    }
    tokio::spawn(toshi);
    info!("Toshi running on {}:{}", &settings.host, &settings.port);

    setup_shutdown(shutdown_signal, index_catalog).await.map_err(Into::into)
}

fn setup_logging(level: &str) {
    std::env::set_var("RUST_LOG", level);
    let sub = tracing_fmt::FmtSubscriber::builder().with_ansi(true).finish();
    tracing::subscriber::set_global_default(sub).expect("Unable to set default Subscriber");
}

async fn setup_shutdown(shutdown_signal: Receiver<()>, index_catalog: SharedCatalog) -> Result<(), oneshot::error::RecvError> {
    shutdown_signal.await?;
    info!("Shutting down...");
    index_catalog.clear().await;
    Ok(())
}

async fn setup_toshi(settings: Settings, index_catalog: SharedCatalog, tx: Sender<()>) -> Result<(), ()> {
    let shutdown = shutdown::shutdown(tx);
    if !settings.experimental_features.leader && settings.experimental {
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
            eprintln!("Error creating IndexCatalog from path {} - {}", settings.path, e);
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
) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>> + Unpin + Send {
    let lock = Arc::new(AtomicBool::new(false));
    let commit_watcher = watcher(Arc::clone(&catalog), settings.auto_commit_duration, Arc::clone(&lock));
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid IP address: {}", &settings.host));

    let bind: SocketAddr = SocketAddr::new(addr, settings.port);
    let root_log = toshi_server::setup_logging();

    let Experimental { id, nodes, leader, .. } = settings.experimental_features;
    let fut = async move {
        let raft_cfg = Config::new(id);
        let peers = Arc::new(DashMap::new());
        let uri = if !nodes.is_empty() {
            format!("http://{}", nodes[0]).parse::<Uri>().unwrap()
        } else {
            Uri::default()
        };

        let raft = ToshiRaft::new(raft_cfg, catalog.base_path(), root_log.clone(), peers.clone(), Arc::clone(&catalog)).unwrap();
        let chan = raft.mailbox_sender.clone();
        let cc = raft.conf_sender.clone();

        tokio::spawn(raft.run());

        if !leader {
            let client: RpcClient = create_client(uri.clone(), Some(root_log.clone())).await.unwrap();
            let req = Request::new(toshi_proto::cluster_rpc::JoinRequest { id, host: uri.to_string() });
            client.clone().join(req).await.unwrap();
            peers.insert(1, client);
        }
        if let Err(e) = RpcServer::<IndexCatalog>::serve(bind, catalog, root_log, chan, cc).await {
            eprintln!("ERROR = {:?}", e);
        }
        Ok(())
    };
    println!("{}", RPC_HEADER);
    info!("I am a data node...Binding to: {}", addr);
    tokio::spawn(commit_watcher);
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
