use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use dashmap::DashMap;
use http::Uri;
use itertools::Itertools;
use log::info;
use raft::Config;
use slog::{o, Drain, Logger};
use slog_async::{Async, OverflowStrategy};
use slog_term::{FullFormat, TermDecorator};

use toshi_raft::rpc_server::RpcServer;
use toshi_raft::Result;
use toshi_server::raft_catalog::RaftCatalog;
use toshi_server::settings::Settings;

#[tokio::main]
pub async fn main() -> Result<()> {
    let decorator = TermDecorator::new().stdout().force_color().build();
    let drain = FullFormat::new(decorator)
        .use_utc_timestamp()
        .build()
        .filter_level(slog::Level::Debug)
        .fuse();
    let drain = Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(OverflowStrategy::Block)
        .build()
        .fuse();
    let logger = Logger::root(drain, o!());
    let _scope = slog_scope::set_global_logger(logger.clone());
    let _guard = slog_stdlog::init_with_level(log::Level::from_str("debug")?)?;

    let args = std::env::args().collect::<Vec<String>>();
    info!("{:?}", args);
    let addr = args[1].parse::<SocketAddr>()?;
    let cluster_nodes: Vec<(&str, &str)> = args[2..].iter().map(|h| h.split(';').collect_tuple().unwrap()).collect();

    let mut settings = Settings::default();
    settings.path = "data2/".into();
    let uris = DashMap::new();
    for (k, v) in cluster_nodes {
        let h = Uri::from_str(v)?;
        uris.insert(k.into(), h);
    }
    let config = Config {
        id: 0,
        election_tick: 0,
        heartbeat_tick: 0,
        applied: 0,
        max_size_per_msg: 0,
        max_inflight_msgs: 0,
        check_quorum: false,
        pre_vote: false,
        min_election_tick: 0,
        max_election_tick: 0,
        read_only_option: Default::default(),
        skip_bcast_commit: false,
        batch_append: false,
        priority: 0,
        max_uncommitted_size: 0,
    };
    let mut catalog = RaftCatalog::new(settings.path.clone().into(), settings.clone(), uris, 1, logger.clone(), config)?;
    catalog.refresh_catalog().await?;

    if let Err(e) = RpcServer::serve(addr, Arc::new(catalog), logger.clone()).await {
        panic!("{}", e);
    }
    Ok(())
}
