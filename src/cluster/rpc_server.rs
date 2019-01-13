#![allow(dead_code, unused_variables)]

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use log::{error, info};
use tokio::executor::DefaultExecutor;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tower_grpc::{Code, Error, Request, Response, Status};
use tower_h2::Server;

use crate::cluster::cluster_rpc::server;
use crate::cluster::cluster_rpc::*;
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

impl RpcServer {
    pub fn get_service(addr: SocketAddr, catalog: Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> {
        let service = server::IndexServiceServer::new(RpcServer { catalog });
        let executor = DefaultExecutor::current();
        let mut h2 = Server::new(service, Default::default(), executor);

        info!("Binding on port: {:?}", addr);
        let bind = TcpListener::bind(&addr).unwrap_or_else(|_| panic!("Failed to bind to host: {:?}", addr));

        info!("Bound to: {:?}", &bind.local_addr().unwrap());
        bind.incoming()
            .for_each(move |sock| {
                let req = h2.serve(sock).map_err(|err| error!("h2 error: {:?}", err));
                tokio::spawn(req);
                Ok(())
            })
            .map_err(|err| error!("Server Error: {:?}", err))
    }

    pub fn create_result(code: i32, message: String) -> ResultReply {
        ResultReply { code, message }
    }

    pub fn create_search_reply(result: Option<ResultReply>, doc: Vec<u8>) -> SearchReply {
        SearchReply { result, doc }
    }
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
            match index.search_index(query) {
                Ok(query_results) => {
                    let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
                    let result = Some(RpcServer::create_result(0, "".into()));
                    let resp = Response::new(RpcServer::create_search_reply(result, query_bytes));
                    Box::new(future::finished(resp))
                }
                Err(e) => {
                    let result = Some(RpcServer::create_result(1, e.to_string()));
                    let resp = Response::new(RpcServer::create_search_reply(result, vec![]));
                    Box::new(future::finished(resp))
                }
            }
        } else {
            let status = Status::with_code_and_message(Code::NotFound, format!("Index: {} not found", inner.index));
            let err = Error::Grpc(status);
            Box::new(future::failed(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::future::Future;

    use crate::cluster::remote_handle::{GrpcConn, RemoteIndex};
    use crate::index::tests::create_test_catalog;
    use crate::query::Query;
    use crate::results::SearchResults;

    use super::*;

    #[test]
    #[ignore]
    fn client_test() {
        let uri: http::Uri = format!("http://localhost:8081").parse().unwrap();
        let socket_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let tcp_stream = GrpcConn(socket_addr);
        let cat = create_test_catalog("test_index");
        let service = RpcServer::get_service(socket_addr, cat);
        let remote = RemoteIndex::new(uri, tcp_stream, "test_index".into());
        let body = r#"test_text:"Duckiment""#;
        let req = query::Request::new(Some(Query::Raw { raw: body.into() }), None, 10);

        let si = remote
            .search_index(req)
            .map(|r| {
                let results: SearchResults = serde_json::from_slice(&r.doc).unwrap();
                assert_eq!(results.hits, 1);
            })
            .map_err(|e| println!("{:?}", e));
        let s = service.select(si).map(|_| ()).map_err(|_| ());

        tokio::run(s);
    }
}
