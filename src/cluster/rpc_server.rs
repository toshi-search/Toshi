use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use log::{error, info};
use tokio::executor::DefaultExecutor;
use tokio::net::{TcpListener, TcpStream};
use tower_grpc::{BoxBody, Code, Error, Request, Response, Status};
use tower_h2::client::{Connect, Connection};
use tower_h2::Server;
use tower_http::add_origin::Builder;
use tower_http::AddOrigin;
use tower_util::MakeService;

use crate::cluster::cluster_rpc::*;
use crate::cluster::cluster_rpc::server;
use crate::cluster::GrpcConn;
use crate::handle::IndexHandle;
use crate::index::IndexCatalog;
use crate::query;

pub type RpcClient = client::IndexService<AddOrigin<Connection<TcpStream, DefaultExecutor, tower_grpc::BoxBody>>>;

/// RPC Services should "ideally" work on only local indexes, they shouldn't be responsible for
/// going to other nodes to get index data. It should be the master's duty to know where the local
/// indexes are stored and make the RPC query to the node to get the data.
#[derive(Clone)]
pub struct RpcServer {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl RpcServer {
    pub fn get_service(addr: SocketAddr, catalog: Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> + Send {
        let service = server::IndexServiceServer::new(RpcServer { catalog });
        let executor = DefaultExecutor::current();

        info!("Binding on port: {:?}", addr);
        let bind = TcpListener::bind(&addr).unwrap_or_else(|_| panic!("Failed to bind to host: {:?}", addr));

        info!("Bound to: {:?}", &bind.local_addr().unwrap());

        let mut h2 = Server::new(service, Default::default(), executor);

            bind.incoming()
                .for_each(move |sock| {
                    let req = h2.serve(sock).map_err(|err| error!("h2 error: {:?}", err));
                    tokio::spawn(req);
                    Ok(())
                })
                .map_err(|err| error!("Server Error: {:?}", err))

    }

    pub fn create_client(conn: GrpcConn, uri: http::Uri) -> Box<Future<Item = RpcClient, Error = Error> + Send> {
        Box::new(
            Connect::new(conn, Default::default(), DefaultExecutor::current())
                .make_service(())
                .map(move |c| {
                    let connection = Builder::new().uri(uri).build(c).unwrap();
                    client::IndexService::new(connection)
                })
                .map_err(|e| {
                    println!("{:?}", e);
                    Error::Inner(())
                }),
        )
    }

    pub fn create_result(code: i32, message: String) -> ResultReply {
        ResultReply { code, message }
    }

    pub fn create_search_reply(result: Option<ResultReply>, doc: Vec<u8>) -> SearchReply {
        SearchReply { result, doc }
    }
}

impl server::IndexService for RpcServer {
    type ListIndexesFuture = Box<Future<Item = Response<ListReply>, Error = Error> + Send>;
    type PlaceIndexFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send>;
    type PlaceDocumentFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send>;
    type PlaceReplicaFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send>;
    type SearchIndexFuture = Box<Future<Item = Response<SearchReply>, Error = Error> + Send>;

    fn place_index(&mut self, request: Request<PlaceRequest>) -> Self::PlaceIndexFuture {
        unimplemented!()
    }

    fn list_indexes(&mut self, _: Request<ListRequest>) -> Self::ListIndexesFuture {
        if let Ok(cat) = self.catalog.read() {
            let indexes: Vec<String> = cat.get_collection().keys().map(|k| k.to_string()).collect();
            let resp = Response::new(ListReply { indexes });
            Box::new(future::finished(resp))
        } else {
            let status = Status::with_code_and_message(Code::NotFound, "Could not get lock on index catalog".into());
            let err = Error::Grpc(status);
            Box::new(future::failed(err))
        }
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
    use http::Uri;
    use tower_grpc::client::unary::ResponseFuture;

    use crate::cluster::remote_handle::RemoteIndex;
    use crate::index::tests::create_test_catalog;
    use crate::query::Query;
    use crate::results::SearchResults;

    use super::*;

    #[test]
    fn client_test() {
        let body = r#"test_text:"Duckiment""#;
        let req = query::Request::new(Some(Query::Raw { raw: body.into() }), None, 10);
        let list = ListRequest {};
        let socket_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let host_uri = Uri::builder()
            .scheme("http")
            .authority(socket_addr.to_string().as_str())
            .path_and_query("")
            .build()
            .unwrap();
        let tcp_stream = GrpcConn(socket_addr);
        let cat = create_test_catalog("test_index");
        let service = RpcServer::get_service(socket_addr, cat);

        let client_fut = RpcServer::create_client(tcp_stream.clone(), host_uri)
            //            .and_then(|client| future::ok(RemoteIndex::new(tcp_stream, "test_index".into(), client)))
            .and_then(move |mut client| {
                client
                    .list_indexes(Request::new(list))
                    .map(|resp| resp.into_inner())
                    .map_err(|r| Error::Inner(()))
            });
        //            .map_err(|e| println!("ERROR = {:?}", e));

        let si = client_fut
            //            .and_then(|remote| {
            //                remote
            //                    .search_index(req)
            //                    .map(|r| {
            //                        let results: SearchResults = serde_json::from_slice(&r.doc).unwrap();
            //                        assert_eq!(results.hits, 1);
            //                    })
            //                    .map_err(|e| println!("{:?}", e))
            //            })
            .map(|e| println!("RES = {:?}", e))
            .map_err(|e| println!("ERROR = {:?}", e));

        let s = service.select(si).map(|_| ()).map_err(|_| ());

        tokio::run(s);
    }
}
