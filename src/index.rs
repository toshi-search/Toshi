use std::clone::Clone;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hashbrown::HashMap;
use http::uri::Scheme;
use http::Uri;
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::Index;
use tokio::sync::Mutex;
use tonic::Status;

use toshi_proto::cluster_rpc::*;
use toshi_types::{DeleteDoc, DocsAffected, Error, Search};

use crate::cluster::remote_handle::RemoteIndex;
use crate::cluster::rpc_server::{RpcClient, RpcServer};
use crate::handle::{IndexHandle, LocalIndex};
use crate::settings::Settings;
use crate::{AddDocument, Result, SearchResults};

pub type SharedCatalog = Arc<tokio::sync::Mutex<IndexCatalog>>;

pub struct IndexCatalog {
    pub settings: Settings,
    base_path: PathBuf,
    local_handles: HashMap<String, LocalIndex>,
    remote_handles: Arc<Mutex<HashMap<String, RemoteIndex>>>,
}

impl IndexCatalog {
    pub fn with_path(base_path: PathBuf) -> Result<Self> {
        IndexCatalog::new(base_path, Settings::default())
    }

    pub fn new(base_path: PathBuf, settings: Settings) -> Result<Self> {
        let remote_idxs = Arc::new(Mutex::new(HashMap::new()));
        let local_idxs = HashMap::new();

        let mut index_cat = IndexCatalog {
            settings,
            base_path,
            local_handles: local_idxs,
            remote_handles: remote_idxs,
        };
        index_cat.refresh_catalog()?;

        Ok(index_cat)
    }

    pub async fn update_remote_indexes(&self) -> Result<()> {
        let hosts = IndexCatalog::refresh_multiple_nodes(self.settings.experimental_features.nodes.clone()).await?;
        for host in hosts {
            for idx in host.1 {
                let ri = RemoteIndex::new(idx.clone(), host.0.clone());
                self.remote_handles.lock().await.insert(idx, ri);
            }
        }
        Ok(())
    }

    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_index(name: String, index: Index) -> Result<Self> {
        let mut map = HashMap::new();
        let remote_map = HashMap::new();
        let new_index = LocalIndex::new(index, Settings::default(), &name)
            .unwrap_or_else(|_| panic!("Unable to open index: {} because it's locked", name));
        map.insert(name, new_index);

        Ok(IndexCatalog {
            settings: Settings::default(),
            base_path: PathBuf::new(),
            local_handles: map,
            remote_handles: Arc::new(Mutex::new(remote_map)),
        })
    }

    pub fn create_from_managed(mut base_path: PathBuf, index_path: &str, schema: Schema) -> Result<Index> {
        base_path.push(index_path);
        if !base_path.exists() {
            fs::create_dir(&base_path).map_err(|e| Error::IOError(e.to_string()))?;
        }
        let dir = MmapDirectory::open(base_path).map_err(|e| Error::IOError(e.to_string()))?;
        Index::open_or_create(dir, schema).map_err(|e| Error::IOError(e.to_string()))
    }

    pub fn load_index(path: &str) -> Result<Index> {
        let p = PathBuf::from(path);
        if p.exists() {
            Index::open_in_dir(&p)
                .map_err(|_| Error::UnknownIndex(p.display().to_string()))
                .and_then(Ok)
        } else {
            Err(Error::UnknownIndex(path.to_string()))
        }
    }

    pub fn add_index(&mut self, name: String, index: Index) -> Result<()> {
        let handle = LocalIndex::new(index, self.settings.clone(), &name)?;
        self.local_handles.insert(name, handle);
        Ok(())
    }

    pub async fn add_remote_index(&mut self, name: String, remote: RpcClient) -> Result<()> {
        let ri = RemoteIndex::new(name.clone(), remote);
        self.remote_handles.lock().await.entry(name).or_insert(ri);
        Ok(())
    }

    pub async fn add_multi_remote_index(&mut self, name: String, remote: Vec<RpcClient>) -> Result<()> {
        let ri = RemoteIndex::with_clients(name.clone(), remote);
        self.remote_handles.lock().await.entry(name).or_insert(ri);
        Ok(())
    }

    pub fn get_collection(&self) -> &HashMap<String, LocalIndex> {
        &self.local_handles
    }

    pub fn get_remote_collection(&self) -> Arc<Mutex<HashMap<String, RemoteIndex>>> {
        Arc::clone(&self.remote_handles)
    }

    pub fn get_mut_collection(&mut self) -> &mut HashMap<String, LocalIndex> {
        &mut self.local_handles
    }

    pub fn exists(&self, index: &str) -> bool {
        self.get_collection().contains_key(index)
    }

    pub async fn remote_exists(&self, index: &str) -> bool {
        self.get_remote_collection().lock().await.contains_key(index)
    }

    pub fn get_mut_index(&mut self, name: &str) -> Result<&mut LocalIndex> {
        self.local_handles.get_mut(name).ok_or_else(|| Error::UnknownIndex(name.into()))
    }

    pub fn get_index(&self, name: &str) -> Result<&LocalIndex> {
        self.local_handles.get(name).ok_or_else(|| Error::UnknownIndex(name.into()))
    }

    pub fn get_owned_index(&self, name: &str) -> Result<LocalIndex> {
        self.local_handles
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownIndex(name.into()))
    }

    pub async fn get_remote_index(&self, name: &str) -> Result<RemoteIndex> {
        self.get_remote_collection()
            .lock()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownIndex(name.into()))
    }

    pub fn refresh_catalog(&mut self) -> Result<()> {
        self.local_handles.clear();

        for dir in fs::read_dir(self.base_path.clone())? {
            let entry = dir?.path();
            if let Some(entry_str) = entry.to_str() {
                if !entry_str.ends_with(".node_id") {
                    let pth: String = entry_str.rsplit('/').take(1).collect();
                    let idx = IndexCatalog::load_index(entry_str)?;
                    self.add_index(pth.clone(), idx)?;
                }
            } else {
                return Err(Error::IOError(format!("Path {} is not a valid unicode path", entry.display())));
            }
        }
        Ok(())
    }

    fn create_host_uri(socket: SocketAddr) -> Result<Uri> {
        Uri::builder()
            .scheme(Scheme::HTTP)
            .authority(socket.to_string().as_str())
            .path_and_query("")
            .build()
            .map_err(|e| Error::IOError(e.to_string()))
    }

    pub async fn create_client(node: String) -> std::result::Result<RpcClient, Error> {
        let socket: SocketAddr = node.parse().unwrap();
        let host_uri = IndexCatalog::create_host_uri(socket)?;
        Ok(RpcServer::create_client(host_uri).await?)
    }

    pub async fn refresh_multiple_nodes(nodes: Vec<String>) -> Result<Vec<(RpcClient, Vec<String>)>> {
        let mut results = vec![];
        for node in nodes {
            let refresh = IndexCatalog::refresh_remote_catalog(node.to_owned())
                .await
                .expect("Could not refresh Index");
            tracing::info!("HOST = {}, INDEXES = {:?}", &node, &refresh.1);
            results.push(refresh);
        }
        Ok(results)
    }

    pub async fn refresh_remote_catalog(node: String) -> std::result::Result<(RpcClient, Vec<String>), Status> {
        let mut client = IndexCatalog::create_client(node).await.expect("Could not create client.");
        let r = client.list_indexes(tonic::Request::new(ListRequest {})).await?.into_inner();
        Ok((client, r.indexes))
    }

    pub async fn search_local_index(&self, index: &str, search: Search) -> Result<SearchResults> {
        let hand = self.get_index(index)?;
        hand.search_index(search).await
    }

    pub async fn search_remote_index(&self, index: &str, search: Search) -> Result<Vec<SearchResults>> {
        let hand = self.get_remote_index(index).await?;
        hand.search_index(search).await.map(|r| vec![r])
    }

    pub async fn add_remote_document(&self, index: &str, doc: AddDocument) -> Result<()> {
        let handle = self.get_remote_index(index).await.map_err(|e| Error::IOError(e.to_string()))?;
        handle.add_document(doc).await
    }

    pub async fn add_local_document(&self, index: &str, doc: AddDocument) -> Result<()> {
        let handle = self.get_owned_index(index).map_err(|e| Error::IOError(e.to_string()))?;
        handle.add_document(doc).await
    }

    pub async fn delete_local_term(&self, index: &str, term: DeleteDoc) -> Result<DocsAffected> {
        let handle = self.get_remote_index(index).await.map_err(|e| Error::IOError(e.to_string()))?;
        handle.delete_term(term).await
    }

    pub async fn clear(&mut self) {
        self.local_handles.clear();
        self.remote_handles.lock().await.clear()
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use super::*;

    pub fn create_test_catalog(name: &str) -> SharedCatalog {
        let idx = toshi_test::create_test_index();
        let catalog = IndexCatalog::with_index(name.into(), idx).unwrap();
        Arc::new(Mutex::new(catalog))
    }
}
