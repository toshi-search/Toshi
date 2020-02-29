use std::hash::{Hash, Hasher};

use rand::prelude::*;
use tracing::*;

use toshi_proto::cluster_rpc::*;
use toshi_proto::cluster_rpc::{DocumentRequest, SearchRequest};
use toshi_types::{DeleteDoc, DocsAffected, Error, Search};

use crate::handlers::fold_results;
use crate::AddDocument;
use crate::SearchResults;
use tantivy::Index;
use toshi_raft::rpc_server::RpcClient;
use toshi_types::{IndexHandle, IndexLocation};

#[derive(Clone)]
pub struct RemoteIndex {
    name: String,
    remotes: Vec<RpcClient>,
}

impl PartialEq for RemoteIndex {
    fn eq(&self, other: &RemoteIndex) -> bool {
        self.name == *other.name
    }
}

impl Eq for RemoteIndex {}

impl Hash for RemoteIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes());
    }
}

impl RemoteIndex {
    pub fn new(name: String, remote: RpcClient) -> Self {
        RemoteIndex::with_clients(name, vec![remote])
    }

    pub fn with_clients(name: String, remotes: Vec<RpcClient>) -> Self {
        Self { name, remotes }
    }
}

#[async_trait::async_trait]
impl IndexHandle for RemoteIndex {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn index_location(&self) -> IndexLocation {
        IndexLocation::REMOTE
    }

    fn get_index(&self) -> Index {
        unimplemented!("Remote indexes do not have indexes to return")
    }

    async fn search_index(&self, search: Search) -> Result<SearchResults, Error> {
        let name = self.get_name();
        let clients = self.remotes.clone();
        info!("REQ = {:?}", search);
        let mut results = vec![];
        for mut client in clients {
            let bytes = serde_json::to_vec(&search)?;
            let req = tonic::Request::new(SearchRequest {
                index: name.clone(),
                query: bytes,
            });
            let search: SearchReply = client.search_index(req).await?.into_inner();
            let search_results: SearchResults = serde_json::from_slice(&search.doc)?;
            results.push(search_results);
        }
        let limit = search.limit;
        Ok(fold_results(results, limit))
    }

    async fn add_document(&self, add: AddDocument) -> Result<(), Error> {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        info!("REQ = {:?}", add);
        let mut random = rand::rngs::SmallRng::from_entropy();
        if let Some(mut client) = clients.choose(&mut random).cloned() {
            let bytes = serde_json::to_vec(&add)?;
            let req = tonic::Request::new(DocumentRequest {
                index: name,
                document: bytes,
            });
            client.place_document(req).await?;
        }
        Ok(())
    }

    async fn delete_term(&self, delete: DeleteDoc) -> Result<DocsAffected, Error> {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        let mut total = 0u64;
        for mut client in clients {
            let bytes = serde_json::to_vec(&delete)?;
            let req = tonic::Request::new(DeleteRequest {
                index: name.clone(),
                terms: bytes,
            });
            let response = client.delete_document(req).await?.into_inner();
            total += response.docs_affected;
        }

        Ok(DocsAffected { docs_affected: total })
    }
}
