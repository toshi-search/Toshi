use std::hash::{Hash, Hasher};

use log::info;
use rand::prelude::*;
use tokio::prelude::*;
use tower_grpc::{Request as TowerRequest, Response};

use toshi_proto::cluster_rpc::{DeleteRequest, DocumentRequest, SearchReply, SearchRequest};

use crate::cluster::rpc_server::RpcClient;
use crate::cluster::RPCError;
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::index::{AddDocument, DeleteDoc};
use crate::query::Request;

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
        state.write(self.name.as_bytes())
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

impl IndexHandle for RemoteIndex {
    type SearchResponse = Box<Future<Item = Vec<SearchReply>, Error = RPCError> + Send>;
    type DeleteResponse = Box<Future<Item = Vec<i32>, Error = RPCError> + Send>;
    type AddResponse = Box<Future<Item = Vec<i32>, Error = RPCError> + Send>;

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn index_location(&self) -> IndexLocation {
        IndexLocation::REMOTE
    }

    fn search_index(&self, search: Request) -> Self::SearchResponse {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        info!("REQ = {:?}", search);
        let fut = clients.into_iter().map(move |mut client| {
            let bytes = match serde_json::to_vec(&search) {
                Ok(v) => v,
                Err(_) => Vec::new(),
            };
            let req = TowerRequest::new(SearchRequest {
                index: name.clone(),
                query: bytes,
            });
            client.search_index(req).map(Response::into_inner).map_err(|e| {
                info!("ERR = {:?}", e);
                e.into()
            })
        });

        Box::new(future::join_all(fut))
    }

    fn add_document(&self, add: AddDocument) -> Self::AddResponse {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        info!("REQ = {:?}", add);
        let mut random = thread_rng();
        let fut = clients.into_iter().choose(&mut random).map(move |mut client| {
            let bytes = match serde_json::to_vec(&add) {
                Ok(v) => v,
                Err(_) => Vec::new(),
            };
            let req = TowerRequest::new(DocumentRequest {
                index: name,
                document: bytes,
            });
            client
                .place_document(req)
                .map(|res| {
                    info!("RESPONSE = {:?}", res);
                    res.into_inner().code
                })
                .map_err(|e| {
                    info!("ERR = {:?}", e);
                    e.into()
                })
        });

        Box::new(future::join_all(fut))
    }

    fn delete_term(&self, delete: DeleteDoc) -> Self::DeleteResponse {
        let name = self.name.clone();
        let clients = self.remotes.clone();
        let fut = clients.into_iter().map(move |mut client| {
            let bytes = match serde_json::to_vec(&delete) {
                Ok(v) => v,
                Err(_) => Vec::new(),
            };
            let req = TowerRequest::new(DeleteRequest {
                index: name.clone(),
                terms: bytes,
            });
            client
                .delete_document(req)
                .map(|res| {
                    info!("RESPONSE = {:?}", res);
                    res.into_inner().code
                })
                .map_err(|e| {
                    info!("ERR = {:?}", e);
                    e.into()
                })
        });

        Box::new(future::join_all(fut))
    }
}
