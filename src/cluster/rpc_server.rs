use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use futures::{future, future::Future, stream::Stream};
use log::{error, info};
use tokio::net::{TcpListener, TcpStream};
use tokio_executor::DefaultExecutor;
use tower_buffer::Buffer;
use tower_grpc::{BoxBody, Code, Error, Request, Response, Status};
use tower_h2::client::{Connect, Connection};
use tower_h2::Server;
use tower_http::add_origin::Builder;
use tower_http::AddOrigin;
use tower_util::MakeService;

use crate::cluster::cluster_rpc::server;
use crate::cluster::cluster_rpc::*;
use crate::cluster::GrpcConn;
use crate::cluster::RPCError;
use crate::handle::IndexHandle;
use crate::index::IndexCatalog;
use crate::query;

pub type Buf = Buffer<AddOrigin<Connection<TcpStream, DefaultExecutor, BoxBody>>, http::Request<BoxBody>>;
pub type RpcClient = client::IndexService<Buf>;

/// RPC Services should "ideally" work on only local indexes, they shouldn't be responsible for
/// going to other nodes to get index data. It should be the master's duty to know where the local
/// indexes are stored and make the RPC query to the node to get the data.
pub struct RpcServer {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl Clone for RpcServer {
    fn clone(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
        }
    }
}

impl RpcServer {
    pub fn get_service(addr: SocketAddr, catalog: Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> {
        let service = server::IndexServiceServer::new(RpcServer { catalog });
        let executor = DefaultExecutor::current();

        info!("Binding on port: {:?}", addr);
        let bind = TcpListener::bind(&addr).unwrap_or_else(|_| panic!("Failed to bind to host: {:?}", addr));

        info!("Bound to: {:?}", &bind.local_addr().unwrap());
        let h2_settings = Default::default();
        let mut h2 = Server::new(service, h2_settings, executor);

        bind.incoming()
            .for_each(move |sock| {
                let req = h2.serve(sock).map_err(|err| error!("h2 error: {:?}", err));
                tokio::spawn(req);
                Ok(())
            })
            .map_err(|err| error!("Server Error: {:?}", err))
    }

    pub fn create_client(conn: GrpcConn, uri: http::Uri) -> impl Future<Item = RpcClient, Error = RPCError> + Send + 'static {
        let mut connect = Connect::new(conn, Default::default(), DefaultExecutor::current());
        let service = connect.make_service(());

        service
            .map(|c| {
                let uri = uri;
                let connection = Builder::new().uri(uri).build(c).unwrap();
                let buffer = match Buffer::new(connection, 0) {
                    Ok(b) => b,
                    _ => panic!("asdf"),
                };
                client::IndexService::new(buffer)
            })
            .map_err(|e| e.into())
    }

    pub fn create_result(code: i32, message: String) -> ResultReply {
        ResultReply { code, message }
    }

    pub fn create_search_reply(result: Option<ResultReply>, doc: Vec<u8>) -> SearchReply {
        SearchReply { result, doc }
    }
}

impl server::IndexService for RpcServer {
    type ListIndexesFuture = future::FutureResult<Response<ListReply>, Error>;
    type PlaceIndexFuture = future::FutureResult<Response<ResultReply>, Error>;
    type PlaceDocumentFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send>;
    type PlaceReplicaFuture = Box<Future<Item = Response<ResultReply>, Error = Error> + Send>;
    type SearchIndexFuture = future::FutureResult<Response<SearchReply>, Error>;

    fn place_index(&mut self, _request: Request<PlaceRequest>) -> Self::PlaceIndexFuture {
        unimplemented!()
    }

    fn list_indexes(&mut self, _: Request<ListRequest>) -> Self::ListIndexesFuture {
        if let Ok(ref mut cat) = self.catalog.read() {
            let indexes = cat.get_collection();
            let lists: Vec<String> = indexes.into_iter().map(|t| t.0.to_string()).collect();
            let resp = Response::new(ListReply { indexes: lists });
            future::finished(resp)
        } else {
            let status = Status::with_code_and_message(Code::NotFound, "Could not get lock on index catalog".into());
            let err = Error::Grpc(status);
            future::failed(err)
        }
    }

    fn place_document(&mut self, _request: Request<DocumentRequest>) -> Self::PlaceDocumentFuture {
        unimplemented!()
    }

    fn place_replica(&mut self, _request: Request<ReplicaRequest>) -> Self::PlaceReplicaFuture {
        unimplemented!()
    }

    fn search_index(&mut self, request: Request<SearchRequest>) -> Self::SearchIndexFuture {
        let inner = request.into_inner();
        if let Ok(ref mut cat) = self.catalog.read() {
            let index = cat.get_index(&inner.index).unwrap();
            let query: query::Request = serde_json::from_slice(&inner.query).unwrap();
            match index.search_index(query) {
                Ok(query_results) => {
                    let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
                    let result = Some(RpcServer::create_result(0, "".into()));
                    let resp = Response::new(RpcServer::create_search_reply(result, query_bytes));
                    future::finished(resp)
                }
                Err(e) => {
                    let result = Some(RpcServer::create_result(1, e.to_string()));
                    let resp = Response::new(RpcServer::create_search_reply(result, vec![]));
                    future::finished(resp)
                }
            }
        } else {
            let status = Status::with_code_and_message(Code::NotFound, format!("Index: {} not found", inner.index));
            let err = Error::Grpc(status);
            future::failed(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::index::tests::create_test_catalog;
    use crate::query::Query;
    use futures::future::Future;
    use http::Uri;

    use super::*;

    #[test]
    #[ignore]
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
            .and_then(|mut client| {
                future::ok(
                    client
                        .list_indexes(Request::new(list))
                        .map(|resp| resp.into_inner())
                        .map_err(|_| ()),
                )
            })
            .map_err(|_| ())
            .and_then(|x| x)
            .map(|x| println!("{:#?}", x))
            .map_err(|_| ());

        let s = service.select(client_fut).map(|_| ()).map_err(|_| ());

        tokio::run(s);
    }
}
