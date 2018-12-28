#![allow(dead_code, unused_variables)]

use std::io::Error as IOError;
use std::net::SocketAddr;

use tokio::executor::DefaultExecutor;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tower_grpc::client::unary::ResponseFuture;
use tower_grpc::Request as TowerRequest;
use tower_h2::client::{Connect, ResponseFuture as RespFut};
use tower_h2::RecvBody;
use tower_http::add_origin::Builder;
use tower_util::MakeService;

use crate::cluster::cluster_rpc::client;
use crate::cluster::cluster_rpc::{Result as RpcResult, SearchReply, SearchRequest};
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::index::{AddDocument, DeleteDoc};
use crate::query::Request;
use crate::settings::Settings;
use tower_h2::client::ConnectError;

/// A reference to an index stored somewhere else on the cluster, this operates via calling
/// the remote host and full filling the request via rpc
#[derive(Clone)]
pub struct RemoteIndex {
    uri: http::Uri,
    rpc_conn: GrpcConn,
    settings: Settings,
    name: String,
}

impl IndexHandle for RemoteIndex {
    type SearchResponse = Box<dyn Future<Item = ResponseFuture<SearchReply, RespFut, RecvBody>, Error = ConnectError<IOError>>>;
    type DeleteResponse = Box<dyn Future<Item = ResponseFuture<RpcResult, RespFut, RecvBody>, Error = ConnectError<IOError>>>;
    type AddResponse = Box<dyn Future<Item = ResponseFuture<RpcResult, RespFut, RecvBody>, Error = ConnectError<IOError>>>;

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn index_location(&self) -> IndexLocation {
        IndexLocation::REMOTE
    }

    fn search_index(&self, search: Request) -> Self::SearchResponse {
        let gconn = self.rpc_conn.clone();
        let uri = self.uri.clone();
        let name = self.name.clone();
        let mut h2_client = Connect::new(gconn, Default::default(), DefaultExecutor::current());

        let rpc_task = h2_client
            .make_service(())
            .map(move |c| {
                let connection = Builder::new().uri(uri).build(c).unwrap();
                client::IndexService::new(connection)
            })
            .and_then(move |mut client| {
                let bytes = serde_json::to_vec(&search).unwrap();
                let req = TowerRequest::new(SearchRequest { index: name, query: bytes });
                Ok(client.search_index(req))
            });

        Box::new(rpc_task)
    }

    fn add_document(&self, doc: AddDocument) -> Self::AddResponse {
        unimplemented!()
    }

    fn delete_term(&self, term: DeleteDoc) -> Self::DeleteResponse {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct GrpcConn(SocketAddr);

impl tokio_connect::Connect for GrpcConn {
    type Connected = TcpStream;
    type Error = IOError;
    type Future = ConnectFuture;

    fn connect(&self) -> Self::Future {
        TcpStream::connect(&self.0)
    }
}
