use std::hash::{Hash, Hasher};

use rand::prelude::*;
use tracing::*;

use toshi_proto::cluster_rpc::{DocumentRequest, SearchRequest};
use toshi_types::query::Search;
use toshi_types::server::{DeleteDoc, DocsAffected};

use crate::cluster::rpc_server::RpcClient;
use crate::handle::{IndexHandle, IndexLocation};
use crate::AddDocument;
use crate::SearchResults;
use toshi_types::error::Error;

/// A reference to an index stored somewhere else on the cluster, this operates via calling
/// the remote host and full filling the request via rpc, we need to figure out a better way
/// (tower-buffer) on how to keep these clients.
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

    async fn search_index(&self, search: Search) -> Result<SearchResults, Error> {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        info!("REQ = {:?}", search);
        //        let client_result = clients
        //            .into_iter()
        //            .map(|mut client| {
        //                let bytes = match serde_json::to_vec(&search) {
        //                    Ok(v) => v,
        //                    Err(_) => Vec::new(),
        //                };
        //                let req = tonic::Request::new(SearchRequest {
        //                    index: name.clone(),
        //                    query: bytes,
        //                });
        ////                client.search_index(req)
        //            })
        //            .collect::<FuturesUnordered<_>>();

        Ok(SearchResults::new(vec![]))
    }

    async fn add_document(&self, add: AddDocument) -> Result<(), Error> {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        info!("REQ = {:?}", add);
        let mut random = rand::rngs::SmallRng::from_entropy();
        let fut = clients.into_iter().choose(&mut random).map(|client| {
            let bytes = match serde_json::to_vec(&add) {
                Ok(v) => v,
                Err(_) => Vec::new(),
            };
            let req = tonic::Request::new(DocumentRequest {
                index: name,
                document: bytes,
            });
            //            client.place_document(req)
            async {}
        });

        fut.unwrap().await;
        Ok(())
    }

    async fn delete_term(&self, _delete: DeleteDoc) -> Result<DocsAffected, Error> {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        //        let fut = clients
        //            .into_iter()
        //            .map(move |mut client| {
        //                let bytes = match serde_json::to_vec(&delete) {
        //                    Ok(v) => v,
        //                    Err(_) => Vec::new(),
        //                };
        //                let req = tonic::Request::new(DeleteRequest {
        //                    index: name.clone(),
        //                    terms: bytes,
        //                });
        //                client.delete_document(req)
        //                //                .map(|res| {
        //                //                    info!("RESPONSE = {:?}", res);
        //                //                    res.into_inner().code
        //                //                })
        //                //                .map_err(|e| {
        //                //                    info!("ERR = {:?}", e);
        //                //                    e
        //                //                })
        //            })
        //            .collect::<FuturesUnordered<_>>();

        Ok(DocsAffected { docs_affected: 0 })
    }
}
