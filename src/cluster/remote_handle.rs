//#![allow(dead_code, unused_variables)]

use h2;
use std::io;
use std::net::SocketAddr;

use tokio::executor::DefaultExecutor;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tower_grpc::{Error, Request as TowerRequest, Response};
use tower_h2::client::{Connect, ConnectError, Error as ConnError};
use tower_http::add_origin::Builder;
use tower_util::MakeService;

use crate::cluster::cluster_rpc::{client, ResultReply, SearchReply, SearchRequest};
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::index::{AddDocument, DeleteDoc};
use crate::query::Request;
use crate::settings::Settings;
use std::error::Error as StdError;
use tower_grpc::client::unary::ResponseFuture;

/// A reference to an index stored somewhere else on the cluster, this operates via calling
/// the remote host and full filling the request via rpc
#[derive(Clone)]
pub struct RemoteIndex {
    uri: http::Uri,
    rpc_conn: GrpcConn,
    settings: Settings,
    name: String,
}

pub enum ServiceError {
    Conn(ConnectError<io::Error>),
    H2Conn(ConnError),
    Grpc(Error),
}

impl IndexHandle for RemoteIndex {
    //    type SearchResponse = Box<Future<Item = ResponseFuture<SearchReply, tower_h2::client::ResponseFuture, tower_h2::RecvBody>, Error = ServiceError<Error>> + Send + 'static>;
    type SearchResponse = Box<ResponseFuture<SearchReply, tower_h2::client::ResponseFuture, tower_h2::RecvBody>>;
    type DeleteResponse = Box<Future<Item = Response<ResultReply>, Error = Error>>;
    type AddResponse = Box<Future<Item = Response<ResultReply>, Error = Error>>;

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

        let rpc_task = h2_client.make_service(());
        let conn_task = rpc_task
            .map(move |c| {
                let connection = Builder::new().uri(uri).build(c).unwrap();
                client::IndexService::new(connection)
            })
            .map_err(|_| Error::Inner(()))
            .and_then(move |mut client| {
                let bytes = serde_json::to_vec(&search).unwrap();
                let req = TowerRequest::new(SearchRequest { index: name, query: bytes });
                Ok(client.search_index(req).map_err(|_| Error::Inner(())))
            });
//            .map(|rf| rf.into());


        Box::new(conn_task)
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
    type Error = io::Error;
    type Future = ConnectFuture;

    fn connect(&self) -> Self::Future {
        TcpStream::connect(&self.0)
    }
}
