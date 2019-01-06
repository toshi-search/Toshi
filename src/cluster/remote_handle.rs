use tokio::prelude::*;
use tower_grpc::{Error, Request as TowerRequest};

use crate::cluster::cluster_rpc::{ResultReply, SearchReply, SearchRequest};
use crate::cluster::rpc_server::RpcClient;
use crate::cluster::GrpcConn;
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::index::{AddDocument, DeleteDoc};
use crate::query::Request;

/// A reference to an index stored somewhere else on the cluster, this operates via calling
/// the remote host and full filling the request via rpc, we need to figure out a better way
/// (tower-buffer) on how to keep these clients.

pub struct RemoteIndex {
    rpc_conn: GrpcConn,
    remote: RpcClient,
    name: String,
}

impl RemoteIndex {
    pub fn new(rpc_conn: &GrpcConn, name: String, remote: &RpcClient) -> Self {
        Self {
            rpc_conn: rpc_conn.clone(),
            name,
            remote: remote.clone(),
        }
    }
}

impl IndexHandle for RemoteIndex {
    type SearchResponse = Box<Future<Item = SearchReply, Error = Error>>;
    type DeleteResponse = Box<Future<Item = ResultReply, Error = Error> + Send>;
    type AddResponse = Box<Future<Item = ResultReply, Error = Error> + Send>;

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn index_location(&self) -> IndexLocation {
        IndexLocation::REMOTE
    }

    fn search_index(&self, search: Request) -> Self::SearchResponse {
        let gconn = self.rpc_conn.clone();
        let name = self.name.clone();
        let client = self.remote.clone();
        println!("GRPC_CONN = {:?}", &gconn);

        let req_task = future::lazy(move || {
            let bytes = serde_json::to_vec(&search).unwrap();
            let req = TowerRequest::new(SearchRequest { index: name, query: bytes });
            client
                .clone()
                .search_index(req)
                .map(|res| {
                    println!("RESPONSE = {:?}", res);
                    res.into_inner()
                })
                .map_err(|e| {
                    println!("{:?}", e);
                    Error::Inner(())
                })
        });

        return Box::new(req_task);
    }

    fn add_document(&self, doc: AddDocument) -> Self::AddResponse {
        unimplemented!()
    }

    fn delete_term(&self, term: DeleteDoc) -> Self::DeleteResponse {
        unimplemented!()
    }
}
