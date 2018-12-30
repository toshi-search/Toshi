use std::sync::Arc;
use std::sync::RwLock;

use tokio::prelude::*;
use tower_grpc::{Code, Error, Request, Response, Status};

use crate::cluster::cluster_rpc::*;
use crate::cluster::cluster_rpc::server;
use crate::handle::IndexHandle;
use crate::index::IndexCatalog;
use crate::query;


/// RPC Services should "ideally" work on only local indexes, they shouldn't be responsible for
/// going to other nodes to get index data. It should be the master's duty to know where the local
/// indexes are stored and make the RPC query to the node to get the data.
#[derive(Clone)]
pub struct RpcServer {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl server::IndexService for RpcServer {
    type PlaceIndexFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send + 'static>;
    type PlaceDocumentFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send + 'static>;
    type PlaceReplicaFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send + 'static>;
    type SearchIndexFuture = Box<Future<Item = Response<SearchReply>, Error = Error> + Send + 'static>;

    fn place_index(&mut self, request: Request<PlaceRequest>) -> Self::PlaceIndexFuture {
        unimplemented!()
    }

    fn place_document(&mut self, request: Request<DocumentRequest>) -> Self::PlaceDocumentFuture {
        unimplemented!()
    }

    fn place_replica(&mut self, request: Request<ReplicaRequest>) -> Self::PlaceReplicaFuture {
        unimplemented!()
    }

    fn search_index(&mut self, request: Request<SearchRequest>) -> Self::SearchIndexFuture {
        let inner = request.into_inner();
        if let Ok(cat) = self.catalog.read() {
            let index = cat.get_index(&inner.index).unwrap();
            let query: query::Request = serde_json::from_slice(&inner.query).unwrap();
            let query_results = index.search_index(query).unwrap();
            let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
            let resp = Response::new(SearchReply {
                result: Some(ResultReply {
                    code: 0,
                    message: "".into(),
                }),
                doc: query_bytes,
            });
            Box::new(future::finished(resp))
        } else {
            Box::new(future::failed(Error::Grpc(Status::with_code_and_message(
                Code::NotFound,
                format!("Index: {} not found", inner.index),
            ))))
        }
    }
}
