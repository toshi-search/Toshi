use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::TryFutureExt;
use http::Uri;
use raft::{Config, RawNode};
use slog::Logger;
use tantivy::schema::Schema;
use tokio::sync::mpsc::channel;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::Request;

use toshi_proto::cluster_rpc::client::IndexServiceClient;
use toshi_proto::cluster_rpc::*;
use toshi_raft::handle::RaftHandle;
use toshi_raft::rpc_utils::create_client;
use toshi_raft::run;
use toshi_types::{Catalog, Error, IndexHandle};

use crate::handle::LocalIndex;
use crate::index::IndexCatalog;
use crate::settings::Settings;

async fn send_create(uri: &Uri, name: String, schema: &Schema) -> toshi_types::Result<()> {
    let mut client: IndexServiceClient<Channel> = create_client(uri, None).await?;
    let schema_json = serde_json::to_vec(&schema)?;
    let place_request = PlaceRequest {
        index: name,
        schema: schema_json,
    };
    let request = Request::new(place_request);
    client.place_index(request).map_ok(|_| ()).map_err(Into::into).await
}

pub struct RaftCatalog<H>
where
    H: IndexHandle + Send + Sync + 'static,
{
    settings: Settings,
    base_path: PathBuf,
    nodes: Arc<DashMap<String, Uri>>,
    handles: DashMap<String, RaftHandle<H>>,
    raft_id: u64,
    logger: Logger,
    config: Config,
}

#[async_trait]
impl Catalog for RaftCatalog<LocalIndex> {
    type Handle = RaftHandle<LocalIndex>;

    fn base_path(&self) -> String {
        format!("{}", self.base_path.display())
    }

    fn get_collection(&self) -> &DashMap<String, Self::Handle> {
        &self.handles
    }

    async fn add_index(&self, name: &str, schema: Schema) -> toshi_types::Result<()> {
        let local = LocalIndex::new(
            self.base_path.clone(),
            name,
            schema.clone(),
            self.settings.writer_memory,
            self.settings.get_merge_policy(),
        )?;
        let (snd, rcv) = channel(1024);
        let arc_snd = Arc::new(snd);
        let raft_handle = RaftHandle::new(local, Arc::clone(&arc_snd));
        let group = RawNode::new(&self.config, raft_handle.clone(), &self.logger).unwrap();
        let arc_rcv = Arc::new(RwLock::new(rcv));

        tokio::spawn(run(group, arc_rcv, Arc::new(RwLock::new(Vec::new())), Arc::clone(&self.nodes)));

        self.handles.insert(name.into(), raft_handle.clone());
        for kv in self.nodes.iter() {
            send_create(kv.value(), name.into(), &schema).await?
        }
        Ok(())
    }

    async fn list_indexes(&self) -> Vec<String> {
        let mut local_keys = self.handles.iter().map(|e| e.key().to_owned()).collect::<Vec<String>>();
        local_keys.sort();
        local_keys.dedup();
        local_keys
    }

    fn get_index(&self, name: &str) -> toshi_types::Result<Self::Handle> {
        self.handles
            .get(name)
            .map(|r| r.value().clone())
            .ok_or_else(|| Error::UnknownIndex(name.into()))
    }

    fn exists(&self, index: &str) -> bool {
        self.get_collection().contains_key(index)
    }

    fn raft_id(&self) -> u64 {
        self.raft_id
    }
}

impl<H> RaftCatalog<H>
where
    H: IndexHandle + Send + Sync + 'static,
{
    pub fn new(
        base_path: PathBuf,
        settings: Settings,
        nodes: DashMap<String, Uri>,
        raft_id: u64,
        logger: Logger,
        config: Config,
    ) -> toshi_raft::Result<Self> {
        Ok(Self {
            settings,
            base_path,
            nodes: Arc::new(nodes),
            handles: DashMap::new(),
            raft_id,
            logger,
            config,
        })
    }
}

impl RaftCatalog<LocalIndex> {
    pub async fn refresh_catalog(&mut self) -> toshi_types::Result<()> {
        self.handles.clear();

        for dir in fs::read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            if let Some(entry_str) = entry.to_str() {
                if entry.exists() {
                    if !entry_str.ends_with(".node_id") {
                        let pth: String = entry_str.rsplit('/').take(1).collect();
                        let idx = IndexCatalog::load_index(entry_str)?;
                        self.add_index(&pth, idx.schema()).await?;
                    }
                } else {
                    return Err(Error::UnknownIndex(format!("Path {}", entry.display())));
                }
            } else {
                return Err(Error::UnknownIndex(format!("Path {} is not a valid unicode path", entry.display())));
            }
        }
        Ok(())
    }
}
