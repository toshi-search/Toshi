use std::io;
use std::net::SocketAddr;

use log::{debug, info};
use tokio::executor::DefaultExecutor;
use tokio::net::tcp::ConnectFuture;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tower_grpc::{Error, Request as TowerRequest};
use tower_h2::client::Connect;
use tower_http::add_origin::Builder;
use tower_util::MakeService;

use crate::cluster::cluster_rpc::{client, ResultReply, SearchReply, SearchRequest};
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::index::{AddDocument, DeleteDoc};
use crate::query::Request;

/// A reference to an index stored somewhere else on the cluster, this operates via calling
/// the remote host and full filling the request via rpc, we need to figure out a better way
/// (tower-buffer) on how to keep these clients.
#[derive(Clone)]
pub struct RemoteIndex {
    uri: http::Uri,
    rpc_conn: GrpcConn,
    name: String,
}

impl RemoteIndex {
    pub fn new(uri: http::Uri, rpc_conn: GrpcConn, name: String) -> Self {
        Self { uri, rpc_conn, name }
    }
}

impl IndexHandle for RemoteIndex {
    type SearchResponse = Box<Future<Item = SearchReply, Error = Error> + Send>;
    type DeleteResponse = Box<Future<Item = ResultReply, Error = Error> + Send>;
    type AddResponse = Box<Future<Item = ResultReply, Error = Error>>;

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
        info!("URI = {:?}, GRPC_CONN = {:?}", uri, &gconn);
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
                client
                    .search_index(req)
                    .map(|res| {
                        debug!("RESPONSE = {:?}", res);
                        res.into_inner()
                    })
                    .map_err(|_| Error::Inner(()))
            });

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
pub struct GrpcConn(pub SocketAddr);

impl tokio_connect::Connect for GrpcConn {
    type Connected = TcpStream;
    type Error = io::Error;
    type Future = ConnectFuture;

    fn connect(&self) -> Self::Future {
        TcpStream::connect(&self.0)
    }
}
