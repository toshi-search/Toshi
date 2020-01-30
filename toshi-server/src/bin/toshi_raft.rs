use dashmap::DashMap;
use http::Uri;
use raft::Config;
use slog::Drain;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use tonic::Request;
use toshi_proto::cluster_rpc::*;
use toshi_raft::raft_node::ToshiRaft;
use toshi_raft::rpc_server::{create_client, RpcClient, RpcServer};
use toshi_server::index::{IndexCatalog, SharedCatalog};
use toshi_server::settings::{Experimental, Settings};
use toshi_server::support;
use toshi_types::Catalog;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let settings = support::settings();
    let addr: IpAddr = settings
        .host
        .parse()
        .unwrap_or_else(|_| panic!("Invalid IP address: {}", &settings.host));

    let host: SocketAddr = SocketAddr::new(addr, settings.port);
    let catalog = setup_catalog(&settings);

    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).use_local_timestamp().build().fuse();
    let async_drain = slog_async::Async::new(drain).build().fuse();
    let root_log = slog::Logger::root(async_drain, slog::o!("toshi" => "toshi"));
    let Experimental { id, nodes, leader, .. } = settings.experimental_features;
    let raft_cfg = Config::new(id);
    let peers = Arc::new(DashMap::new());
    let uri = if !nodes.is_empty() {
        format!("http://{}", nodes[0]).parse::<Uri>()?
    } else {
        Uri::default()
    };

    let raft = ToshiRaft::new(&raft_cfg, catalog.base_path(), &root_log, peers.clone())?;
    let chan = raft.mailbox_sender.clone();
    let cc = raft.conf_sender.clone();

    tokio::spawn(raft.run());

    if !leader {
        let client: RpcClient = create_client(uri, Some(root_log.clone())).await?;
        let req = Request::new(JoinRequest {
            id,
            host: host.to_string(),
        });
        client.clone().join(req).await.unwrap();
        peers.insert(1, client);
    }

    if let Err(e) = RpcServer::serve(host, catalog, root_log, chan, cc).await {
        eprintln!("ERROR = {:?}", e);
    }
    Ok(())
}

fn setup_catalog(settings: &Settings) -> SharedCatalog {
    let path = PathBuf::from(&settings.path);
    let index_catalog = IndexCatalog::new(path, settings.clone()).unwrap();
    Arc::new(index_catalog)
}
