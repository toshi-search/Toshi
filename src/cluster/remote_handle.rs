use log::info;
use tokio::prelude::*;
use tower_grpc::Request as TowerRequest;

use crate::cluster::cluster_rpc::{ResultReply, SearchReply, SearchRequest};
use crate::cluster::rpc_server::RpcClient;
use crate::cluster::RPCError;
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::index::{AddDocument, DeleteDoc};
use crate::query::Request;

/// A reference to an index stored somewhere else on the cluster, this operates via calling
/// the remote host and full filling the request via rpc, we need to figure out a better way
/// (tower-buffer) on how to keep these clients.

pub struct RemoteIndex {
    name: String,
    remote: RpcClient,
}

impl RemoteIndex {
    pub fn new(name: String, remote: RpcClient) -> Self {
        Self { name, remote }
    }
}

impl IndexHandle for RemoteIndex {
    type SearchResponse = Box<Future<Item = SearchReply, Error = RPCError> + Send>;
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
        let mut client = self.remote.clone();
        let bytes = serde_json::to_vec(&search).unwrap();
        let req = TowerRequest::new(SearchRequest { index: name, query: bytes });
        let fut = client
            .search_index(req)
            .map(|res| {
                info!("RESPONSE = {:?}", res);
                res.into_inner()
            })
            .map_err(|e| {
                info!("{:?}", e);
                e.into()
            });

        Box::new(fut)
    }

    fn add_document(&self, _: AddDocument) -> Self::AddResponse {
        unimplemented!()
    }

    fn delete_term(&self, _: DeleteDoc) -> Self::DeleteResponse {
        unimplemented!()
    }
}
