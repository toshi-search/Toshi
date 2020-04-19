use std::error::Error;
use std::fs::create_dir;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{atomic::AtomicBool, Arc};

use dashmap::DashMap;
use futures::prelude::*;
use http::Uri;
use log::info;
use raft::Config;
use tokio::sync::{mpsc, oneshot};
use tonic::Request;

use toshi_proto::cluster_rpc::Message;
use toshi_raft::raft_node::ToshiRaft;
use toshi_raft::rpc_server::{create_client, RpcClient, RpcServer};
use toshi_server::commit::watcher;
use toshi_server::index::{IndexCatalog, SharedCatalog};
use toshi_server::router::Router;
use toshi_server::settings::{settings, Experimental, Settings, HEADER};
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
    if settings.experimental && settings.experimental_features.leader {
        let update_cat = Arc::clone(&index_catalog);
        tokio::spawn(async move { update_cat.update_remote_indexes().await });
    }
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
        let (sender, recv) = tokio::sync::mpsc::channel(1024);
        let master = run_master(Arc::clone(&index_catalog), settings.clone(), Some(sender.clone()));
        tokio::spawn(run_data(Arc::clone(&index_catalog), settings, sender.clone(), recv));
        future::try_select(shutdown, master).map(|_| Ok(())).await
    } else {
        let master = run_master(Arc::clone(&index_catalog), settings, None);
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
    sender: mpsc::Sender<Message>,
    recv: mpsc::Receiver<Message>,
) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync + 'static>>> + Unpin + Send {
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid IP address: {}", &settings.host));

    let bind: SocketAddr = SocketAddr::new(addr, settings.experimental_features.rpc_port);

    let Experimental {
        id,
        nodes,
        leader,
        rpc_port,
        ..
    } = settings.experimental_features;
    let resp_uri = format!("http://{}:{}", settings.host, rpc_port);

    let fut = async move {
        let raft_cfg = Config::new(id);
        let peers = Arc::new(DashMap::new());
        let uri = if !nodes.is_empty() {
            format!("http://{}", nodes[0]).parse::<Uri>()?
        } else {
            Uri::default()
        };

        let raft = ToshiRaft::new(
            raft_cfg,
            catalog.base_path(),
            slog_scope::logger(),
            peers.clone(),
            Arc::clone(&catalog),
            sender,
            recv,
        )?;
        let chan = raft.mailbox_sender.clone();
        let cc = raft.conf_sender.clone();

        tokio::spawn(raft.run());

        if !leader {
            let client: RpcClient = create_client(uri.clone(), Some(slog_scope::logger())).await?;
            let req = Request::new(toshi_proto::cluster_rpc::JoinRequest {
                id,
                host: resp_uri.to_string(),
            });
            client.clone().join(req).await?;
            peers.insert(1, client);
        }
        info!("I am a data node...Binding to: {}", bind);

        if let Err(e) = RpcServer::<IndexCatalog>::serve(bind, catalog, slog_scope::logger(), chan, cc).await {
            eprintln!("ERROR = {:?}", e);
        }
        Ok(())
    };
    // println!("{}", RPC_HEADER);
    // tokio::spawn(commit_watcher);
    Box::pin(fut)
}

fn run_master(
    catalog: SharedCatalog,
    settings: Settings,
    sender: Option<mpsc::Sender<Message>>,
) -> impl Future<Output = Result<(), hyper::Error>> + Unpin + Send {
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
    let router = Router::new(catalog, watcher_clone, sender);
    Box::pin(router.router_with_catalog(bind))
}
