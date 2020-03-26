use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use dashmap::DashMap;
use http::Uri;
use raft::Config;
use tonic::Request;

use toshi_proto::cluster_rpc::*;
use toshi_raft::raft_node::ToshiRaft;
use toshi_raft::rpc_server::{create_client, RpcClient, RpcServer};
use toshi_server::settings::{settings, Experimental};
use toshi_server::{setup_catalog, setup_logging_from_file};
use toshi_types::Catalog;
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let settings = settings();
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid IP address: {}", &settings.host));

    let host: SocketAddr = SocketAddr::new(addr, settings.port);
    let catalog = setup_catalog(&settings);

    let logger = setup_logging_from_file("config/logging.toml")?;
    let _scope = slog_scope::set_global_logger(logger.clone());
    let _guard = slog_stdlog::init_with_level(log::Level::from_str(&settings.log_level)?)?;

    let Experimental { id, nodes, leader, .. } = settings.experimental_features;
    let raft_cfg = Config::new(id);
    let peers: Arc<DashMap<u64, RpcClient>> = Arc::new(DashMap::new());
    let uri = if !nodes.is_empty() {
        format!("http://{}", nodes[0]).parse::<Uri>()?
    } else {
        Uri::default()
    };

    let (sender, recv) = tokio::sync::mpsc::channel(1024);
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

    if !leader {
        let client: RpcClient = create_client(uri, Some(slog_scope::logger())).await?;
        let req = Request::new(JoinRequest {
            id,
            host: host.to_string(),
        });
        let resp = client.clone().join(req).await;
        log::info!("RESP_JOIN = {:?}", resp);
        peers.insert(1, client);
    }

    tokio::spawn(raft.run());
    log::info!("HOST = {:?}", host);

    if let Err(e) = RpcServer::serve(host, catalog, slog_scope::logger(), chan, cc).await {
        eprintln!("ERROR = {:?}", e);
    }
    Ok(())
}
