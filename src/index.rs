use std::collections::HashMap;
use std::fs;
use std::iter::Iterator;
use std::net::SocketAddr;
use std::path::PathBuf;

use futures::Future;
use http::Uri;
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::Index;

use crate::cluster::cluster_rpc::ListRequest;
use crate::cluster::remote_handle::RemoteIndex;
use crate::cluster::rpc_server::RpcClient;
use crate::cluster::rpc_server::RpcServer;
use crate::cluster::GrpcConn;
use crate::cluster::RPCError;
use crate::handle::{IndexHandle, LocalIndex};
use crate::query::Request;
use crate::results::*;
use crate::settings::Settings;
use crate::{Error, Result};

pub struct IndexCatalog {
    pub settings: Settings,
    base_path: PathBuf,
    local_indexes: HashMap<String, LocalIndex>,
    remote_indexes: HashMap<String, RemoteIndex>,
}

impl IndexCatalog {
    pub fn with_path(base_path: PathBuf) -> Result<Self> {
        IndexCatalog::new(base_path, Settings::default())
    }

    pub fn new(base_path: PathBuf, settings: Settings) -> Result<Self> {
        let mut index_cat = IndexCatalog {
            settings,
            base_path,
            local_indexes: HashMap::new(),
            remote_indexes: HashMap::new(),
        };
        index_cat.refresh_catalog()?;
        Ok(index_cat)
    }

    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_index(name: String, index: Index) -> Result<Self> {
        let mut map = HashMap::new();
        let new_index = LocalIndex::new(index, Settings::default(), &name)
            .unwrap_or_else(|_| panic!("Unable to open index: {} because it's locked", name));
        map.insert(name, new_index);
        Ok(IndexCatalog {
            settings: Settings::default(),
            base_path: PathBuf::new(),
            local_indexes: map,
            remote_indexes: HashMap::new(),
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
        self.local_indexes.insert(name.clone(), handle);
        Ok(())
    }

    pub fn add_remote_index(&mut self, name: String, remote: RpcClient) -> Result<()> {
        let ri = RemoteIndex::new(name.clone(), remote);
        self.remote_indexes.entry(name).or_insert(ri);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_collection(&self) -> &HashMap<String, LocalIndex> {
        &self.local_indexes
    }

    pub fn get_mut_collection(&mut self) -> &mut HashMap<String, LocalIndex> {
        &mut self.local_indexes
    }

    pub fn exists(&self, index: &str) -> bool {
        self.get_collection().contains_key(&index.to_string())
    }

    pub fn get_mut_index(&mut self, name: &str) -> Result<&mut LocalIndex> {
        self.local_indexes.get_mut(name).ok_or_else(|| Error::UnknownIndex(name.into()))
    }

    pub fn get_index(&self, name: &str) -> Result<&LocalIndex> {
        self.local_indexes.get(name).ok_or_else(|| Error::UnknownIndex(name.into()))
    }

    pub fn refresh_catalog(&mut self) -> Result<()> {
        self.local_indexes.clear();

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
            .scheme("http")
            .authority(socket.to_string().as_str())
            .path_and_query("")
            .build()
            .map_err(|e| Error::IOError(e.to_string()))
    }

    pub fn refresh_remote_catalog(node: String) -> impl Future<Item = (), Error = RPCError> + Send + 'static {
        let socket: SocketAddr = node.parse().unwrap();
        let host_uri = IndexCatalog::create_host_uri(socket).unwrap();
        let grpc_conn = GrpcConn(socket);
        let client_fut = RpcServer::create_client(grpc_conn.clone(), host_uri)
            .and_then(|mut client| {
                client
                    .list_indexes(tower_grpc::Request::new(ListRequest {}))
                    .map(|resp| resp.into_inner())
                    .inspect(|x| println!("{:#?}", x))
                    .map_err(|e| e.into())
            })
            .map(|x| {
                // Uhh... do stuff here that actually matters...
                println!("{:#?}", x)
            });

        client_fut
    }

    pub fn search_index(&self, index: &str, search: Request) -> Result<SearchResults> {
        match self.get_index(index) {
            Ok(hand) => hand.search_index(search),
            Err(e) => Err(e),
        }
    }

    pub fn clear(&mut self) {
        self.local_indexes.clear();
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::{Arc, RwLock};

    use tantivy::doc;
    use tantivy::schema::*;

    use super::*;

    pub fn create_test_catalog(name: &str) -> Arc<RwLock<IndexCatalog>> {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index(name.into(), idx).unwrap();
        Arc::new(RwLock::new(catalog))
    }

    pub fn create_test_index() -> Index {
        let mut builder = SchemaBuilder::new();
        let test_text = builder.add_text_field("test_text", STORED | TEXT);
        let test_int = builder.add_i64_field("test_i64", INT_STORED | INT_INDEXED);
        let test_unsign = builder.add_u64_field("test_u64", INT_STORED | INT_INDEXED);
        let test_unindexed = builder.add_text_field("test_unindex", STORED);

        let schema = builder.build();
        let idx = Index::create_in_ram(schema);
        let mut writer = idx.writer(30_000_000).unwrap();
        writer.add_document(doc! { test_text => "Test Document 1", test_int => 2014i64,  test_unsign => 10u64, test_unindexed => "no" });
        writer.add_document(doc! { test_text => "Test Dockument 2", test_int => -2015i64, test_unsign => 11u64, test_unindexed => "yes" });
        writer.add_document(doc! { test_text => "Test Duckiment 3", test_int => 2016i64,  test_unsign => 12u64, test_unindexed => "noo" });
        writer.add_document(doc! { test_text => "Test Document 4", test_int => -2017i64, test_unsign => 13u64, test_unindexed => "yess" });
        writer.add_document(doc! { test_text => "Test Document 5", test_int => 2018i64,  test_unsign => 14u64, test_unindexed => "nooo" });
        writer.commit().unwrap();

        idx
    }

    #[test]
    #[ignore]
    pub fn test_remote_index_refresh() {
        let socket_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let cat = create_test_catalog("test_index");
        let service = RpcServer::get_service(socket_addr, cat);
        let nodes = "127.0.0.1:8081".into();
        let refresh = IndexCatalog::refresh_remote_catalog(nodes).map_err(|_| ());

        let s = service.select(refresh).map(|_| ()).map_err(|_| ());

        tokio::run(s);
    }
}
