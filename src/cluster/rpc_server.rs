use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use crate::cluster::cluster_rpc::*;
use log::{error, info};
use tantivy::schema::Schema;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_executor::DefaultExecutor;
use tower_grpc::{BoxBody, Code, Request, Response, Status};
use tower_util::MakeService;

use crate::cluster::GrpcConn;
use crate::handle::IndexHandle;
use crate::index::IndexCatalog;
use crate::query;
use tower_add_origin::{AddOrigin, Builder};
use tower_buffer::Buffer;
use tower_h2::client::{Connect, ConnectError, Connection};
use tower_h2::Server;

//pub type RpcClient = client::IndexService<Buffer<Connection<Body>, http::Request<Body>>>;

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
        //let mut hyp = Server::new(service);
        let mut h2 = Server::new(service, Default::default(), executor);

        bind.incoming()
            .for_each(move |sock| {
                let req = h2.serve(sock).map_err(|err| error!("h2 error: {:?}", err));
                tokio::spawn(req);
                Ok(())
            })
            .map_err(|err| error!("Server Error: {:?}", err))
    }

    pub fn create_client(
        conn: GrpcConn,
        uri: http::Uri,
    ) -> impl Future<Item = RpcClient, Error = ConnectError<::std::io::Error>> + Send + 'static {
        let mut connect = Connect::new(conn, Default::default(), DefaultExecutor::current());

        connect.make_service(()).map(|c| {
            let uri = uri;
            let connection = Builder::new().uri(uri).build(c).unwrap();
            let buffer = match Buffer::new(connection, 128) {
                Ok(b) => b,
                _ => panic!("asdf"),
            };
            client::IndexService::new(buffer)
        })
    }

    pub fn create_result(code: i32, message: String) -> ResultReply {
        ResultReply { code, message }
    }

    pub fn create_search_reply(result: Option<ResultReply>, doc: Vec<u8>) -> SearchReply {
        SearchReply { result, doc }
    }

    pub fn error_response<T>(code: Code, msg: String) -> Box<future::FutureResult<Response<T>, Status>> {
        let status = Status::new(code, msg);
        Box::new(future::failed(status))
    }
}

impl server::IndexService for RpcServer {
    type ListIndexesFuture = Box<future::FutureResult<Response<ListReply>, Status>>;
    type PlaceIndexFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;
    type PlaceDocumentFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;
    type PlaceReplicaFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;
    type SearchIndexFuture = Box<future::FutureResult<Response<SearchReply>, Status>>;

    fn list_indexes(&mut self, _: Request<ListRequest>) -> Self::ListIndexesFuture {
        if let Ok(ref cat) = self.catalog.read() {
            let indexes = cat.get_collection();
            let lists: Vec<String> = indexes.into_iter().map(|(t, _)| t.to_string()).collect();
            let resp = Response::new(ListReply { indexes: lists });
            Box::new(future::finished(resp))
        } else {
            Self::error_response(Code::NotFound, "Could not get lock on index catalog".into())
        }
    }

    fn search_index(&mut self, request: Request<SearchRequest>) -> Self::SearchIndexFuture {
        let inner = request.into_inner();
        if let Ok(ref cat) = self.catalog.read() {
            if let Ok(index) = cat.get_index(&inner.index) {
                let query: query::Request = serde_json::from_slice(&inner.query).unwrap();
                info!("QUERY = {:?}", query);
                match index.search_index(query) {
                    Ok(query_results) => {
                        let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
                        let result = Some(RpcServer::create_result(0, "".into()));
                        Box::new(future::finished(Response::new(RpcServer::create_search_reply(result, query_bytes))))
                    }
                    Err(e) => {
                        let result = Some(RpcServer::create_result(1, e.to_string()));
                        Box::new(future::finished(Response::new(RpcServer::create_search_reply(result, vec![]))))
                    }
                }
            } else {
                Self::error_response(Code::NotFound, format!("Index: {} not found", inner.index))
            }
        } else {
            Self::error_response(
                Code::NotFound,
                format!("Could not obtain lock on catalog for index: {}", inner.index),
            )
        }
    }

    fn place_index(&mut self, request: Request<PlaceRequest>) -> Self::PlaceIndexFuture {
        let PlaceRequest { index, schema } = request.into_inner();
        if let Ok(ref mut cat) = self.catalog.write() {
            if let Ok(schema) = serde_json::from_slice::<Schema>(&schema) {
                let ip = cat.base_path().clone();
                if let Ok(new_index) = IndexCatalog::create_from_managed(ip, &index.clone(), schema) {
                    if cat.add_index(index.clone(), new_index).is_ok() {
                        let result = RpcServer::create_result(0, "".into());
                        Box::new(future::finished(Response::new(result)))
                    } else {
                        Self::error_response(Code::Internal, format!("Insert: {} failed", index.clone()))
                    }
                } else {
                    Self::error_response(Code::Internal, format!("Could not create index: {}", index.clone()))
                }
            } else {
                Self::error_response(Code::NotFound, "Invalid schema in request".into())
            }
        } else {
            Self::error_response(Code::NotFound, format!("Cannot obtain lock on catalog for index: {}", index))
        }
    }

    fn place_document(&mut self, _request: Request<DocumentRequest>) -> Self::PlaceDocumentFuture {
        unimplemented!()
    }

    fn place_replica(&mut self, _request: Request<ReplicaRequest>) -> Self::PlaceReplicaFuture {
        unimplemented!()
    }
}

//impl<T> Service<http::Request<Body>> for server::IndexServiceServer<T> {
//    type Response = http::Response<LiftBody<Body>>;
//    type Error = h2::Error;
//    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;
//
//    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
//        unimplemented!()
//    }
//
//    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
//        let fut = self.make_service(()).call(req);
//        Box::new(fut)
//    }
//}
//
//#[cfg(test)]
//mod tests {
//    use futures::future::Future;
//    use http::Uri;
//
//    use crate::index::tests::create_test_catalog;
//    use crate::query::Query;
//
//    use super::*;
//
//    #[test]
//    #[ignore]
//    fn client_test() {
//        let body = r#"test_text:"Duckiment""#;
//        let _req = query::Request::new(Some(Query::Raw { raw: body.into() }), None, 10);
//        let list = ListRequest {};
//        let socket_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();
//
//        let host_uri = Uri::builder()
//            .scheme("http")
//            .authority(socket_addr.to_string().as_str())
//            .path_and_query("")
//            .build()
//            .unwrap();
//
//        let tcp_stream = GrpcConn(socket_addr);
//        let cat = create_test_catalog("test_index");
//        let service = RpcServer::get_service(socket_addr, cat);
//
//        let client_fut = RpcServer::create_client(tcp_stream.clone(), host_uri)
//            .and_then(|mut client| {
//                future::ok(
//                    client
//                        .list_indexes(Request::new(list))
//                        .map(|resp| resp.into_inner())
//                        .map_err(|_| ()),
//                )
//            })
//            .map_err(|_| ())
//            .and_then(|x| x)
//            .map(|x| println!("{:#?}", x))
//            .map_err(|_| ());
//
//        let s = service.select(client_fut).map(|_| ()).map_err(|_| ());
//
//        tokio::run(s);
//    }
//}
