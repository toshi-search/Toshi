use std::hash::{Hash, Hasher};

use rand::prelude::*;
use tonic::Response;
use tracing::*;

use toshi_proto::cluster_rpc::*;
use toshi_proto::cluster_rpc::{DocumentRequest, SearchRequest};
use toshi_types::error::Error;
use toshi_types::query::Search;
use toshi_types::server::{DeleteDoc, DocsAffected};

use crate::cluster::rpc_server::RpcClient;
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::fold_results;
use crate::AddDocument;
use crate::SearchResults;

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
        let name = self.get_name();
        let clients = self.remotes.clone();
        info!("REQ = {:?}", search);
        let mut results = vec![];
        for mut client in clients {
            let bytes = match serde_json::to_vec(&search) {
                Ok(v) => v,
                Err(_) => Vec::new(),
            };
            let req = tonic::Request::new(SearchRequest {
                index: name.clone(),
                query: bytes,
            });
            let search: Response<SearchReply> = client.search_index(req).await.unwrap();
            let reply: SearchReply = search.into_inner();
            let search_results: SearchResults = serde_json::from_slice(&reply.doc)?;
            results.push(search_results);
        }
        Ok(fold_results(results))
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
