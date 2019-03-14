use std::hash::{Hash, Hasher};

use log::info;
use tokio::prelude::*;
use tower_grpc::Request as TowerRequest;

use toshi_proto::cluster_rpc::{ResultReply, SearchReply, SearchRequest};

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
        Self {
            name,
            remotes: vec![remote],
        }
    }
}

impl IndexHandle for RemoteIndex {
    type SearchResponse = Box<Future<Item = Vec<SearchReply>, Error = RPCError> + Send>;
    type DeleteResponse = Box<Future<Item = ResultReply, Error = RPCError> + Send>;
    type AddResponse = Box<Future<Item = ResultReply, Error = RPCError> + Send>;

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
            let bytes = serde_json::to_vec(&search).unwrap();
            let req = TowerRequest::new(SearchRequest {
                index: name.clone(),
                query: bytes,
            });
            client
                .search_index(req)
                .map(|res| {
                    info!("RESPONSE = {:?}", res);
                    res.into_inner()
                })
                .map_err(|e| {
                    info!("ERR = {:?}", e);
                    e.into()
                })
        });

        Box::new(future::join_all(fut))
    }

    fn add_document(&self, _: AddDocument) -> Self::AddResponse {
        unimplemented!("All of the mutating calls should probably have some strategy to balance how they distribute documents and knowing where things are")
    }

    fn delete_term(&self, _: DeleteDoc) -> Self::DeleteResponse {
        unimplemented!()
    }
}
