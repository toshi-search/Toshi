use std::io::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::Future;
use hyper::client::connect::HttpConnector;
use parking_lot::RwLock;
use tantivy::schema::Schema;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tower::MakeService;
use tower_buffer::Buffer;
use tower_grpc::{BoxBody, Code, Request, Response, Status, Streaming};
use tower_hyper::client::{Connect, ConnectError, Connection};
use tower_hyper::util::{Connector, Destination};
use tower_hyper::Server;
use tower_request_modifier::{Builder, RequestModifier};
use tracing::*;

use toshi_proto::cluster_rpc::*;
use toshi_types::query::Search;

use crate::handle::IndexHandle;
use crate::index::IndexCatalog;
use crate::AddDocument;
use toshi_types::server::DeleteDoc;

pub type Buf = Buffer<RequestModifier<Connection<BoxBody>, BoxBody>, http::Request<BoxBody>>;
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
    pub fn serve(addr: SocketAddr, catalog: Arc<RwLock<IndexCatalog>>) -> impl Future<Item = (), Error = ()> {
        let service = server::IndexServiceServer::new(RpcServer { catalog });
        info!("Binding on port: {:?}", addr);
        let bind = TcpListener::bind(&addr).unwrap_or_else(|_| panic!("Failed to bind to host: {:?}", addr));

        info!("Bound to: {:?}", &bind.local_addr().unwrap());
        let mut hyp = Server::new(service);

        bind.incoming()
            .for_each(move |sock| {
                info!("Connection from: {:?}", sock.local_addr().unwrap());
                let req = hyp.serve(sock).map_err(|err| error!("hyper error: {:?}", err));
                tokio::spawn(req);
                Ok(())
            })
            .map_err(|err| error!("Server Error: {:?}", err))
    }

    //TODO: Make DNS Threads and Buffer Requests Configurable options
    pub fn create_client(uri: http::Uri) -> impl Future<Item = RpcClient, Error = ConnectError<Error>> {
        info!("Creating Client to: {:?}", uri);
        let dst = Destination::try_from_uri(uri.clone()).unwrap();
        let connector = Connector::new(HttpConnector::new(num_cpus::get()));
        let mut connect = Connect::new(connector);

        connect.make_service(dst).map(move |c| {
            let connection = Builder::new().set_origin(uri).build(c).unwrap();
            let buffer = Buffer::new(connection, 128);
            client::IndexService::new(buffer)
        })
    }

    pub fn ok_result() -> ResultReply {
        RpcServer::create_result(0, "".into())
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
    type PingFuture = Box<future::FutureResult<Response<PingReply>, Status>>;
    type ListIndexesFuture = Box<future::FutureResult<Response<ListReply>, Status>>;
    type PlaceIndexFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;
    type PlaceDocumentFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;
    type PlaceReplicaFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;
    type SearchIndexFuture = Box<future::FutureResult<Response<SearchReply>, Status>>;
    type DeleteDocumentFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;
    type GetSummaryFuture = Box<future::FutureResult<Response<SummaryReply>, Status>>;
    type BulkInsertFuture = Box<future::FutureResult<Response<ResultReply>, Status>>;

    fn list_indexes(&mut self, req: Request<ListRequest>) -> Self::ListIndexesFuture {
        let cat = self.catalog.read();
        info!("Request From: {:?}", req);
        let indexes = cat.get_collection();
        let lists: Vec<String> = indexes.into_iter().map(|(t, _)| t.to_string()).collect();
        info!("Response: {:?}", lists.join(", "));
        let resp = Response::new(ListReply { indexes: lists });
        Box::new(future::finished(resp))
    }

    fn search_index(&mut self, request: Request<SearchRequest>) -> Self::SearchIndexFuture {
        let inner = request.into_inner();
        let cat = self.catalog.read();
        if let Ok(index) = cat.get_index(&inner.index) {
            let query: Search = match serde_json::from_slice(&inner.query) {
                Ok(v) => v,
                Err(e) => return Self::error_response(Code::Internal, e.to_string()),
            };
            info!("QUERY = {:?}", query);

            match index.search_index(query) {
                Ok(query_results) => {
                    info!("Query Response = {:?} hits", query_results.hits);
                    let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
                    let result = Some(RpcServer::ok_result());
                    Box::new(future::finished(Response::new(RpcServer::create_search_reply(result, query_bytes))))
                }
                Err(e) => {
                    info!("Query Response = {:?}", e);
                    let result = Some(RpcServer::create_result(1, e.to_string()));
                    Box::new(future::finished(Response::new(RpcServer::create_search_reply(result, vec![]))))
                }
            }
        } else {
            Self::error_response(Code::NotFound, format!("Index: {} not found", inner.index))
        }
    }

    fn place_index(&mut self, request: Request<PlaceRequest>) -> Self::PlaceIndexFuture {
        let PlaceRequest { index, schema } = request.into_inner();
        let mut cat = self.catalog.write();
        if let Ok(schema) = serde_json::from_slice::<Schema>(&schema) {
            let ip = cat.base_path().clone();
            if let Ok(new_index) = IndexCatalog::create_from_managed(ip, &index.clone(), schema) {
                if cat.add_index(index.clone(), new_index).is_ok() {
                    Box::new(future::finished(Response::new(RpcServer::ok_result())))
                } else {
                    Self::error_response(Code::Internal, format!("Insert: {} failed", index.clone()))
                }
            } else {
                Self::error_response(Code::Internal, format!("Could not create index: {}", index.clone()))
            }
        } else {
            Self::error_response(Code::NotFound, "Invalid schema in request".into())
        }
    }

    fn place_document(&mut self, request: Request<DocumentRequest>) -> Self::PlaceDocumentFuture {
        let DocumentRequest { index, document } = request.into_inner();
        let cat = self.catalog.read();
        if let Ok(idx) = cat.get_index(&index) {
            if let Ok(doc) = serde_json::from_slice::<AddDocument>(&document) {
                if idx.add_document(doc).is_ok() {
                    Box::new(future::finished(Response::new(RpcServer::ok_result())))
                } else {
                    Self::error_response(Code::Internal, format!("Add Document Failed: {}", index))
                }
            } else {
                Self::error_response(Code::Internal, format!("Invalid Document request: {}", index))
            }
        } else {
            Self::error_response(Code::NotFound, "Could not find index".into())
        }
    }

    fn place_replica(&mut self, _request: Request<ReplicaRequest>) -> Self::PlaceReplicaFuture {
        unimplemented!()
    }

    fn delete_document(&mut self, request: Request<DeleteRequest>) -> Self::DeleteDocumentFuture {
        let DeleteRequest { index, terms } = request.into_inner();
        let cat = self.catalog.read();
        if let Ok(idx) = cat.get_index(&index) {
            if let Ok(delete_docs) = serde_json::from_slice::<DeleteDoc>(&terms) {
                if idx.delete_term(delete_docs).is_ok() {
                    Box::new(future::finished(Response::new(RpcServer::ok_result())))
                } else {
                    Self::error_response(Code::Internal, format!("Add Document Failed: {}", index))
                }
            } else {
                Self::error_response(Code::Internal, format!("Invalid Document request: {}", index))
            }
        } else {
            Self::error_response(Code::NotFound, "Could not find index".into())
        }
    }

    fn get_summary(&mut self, request: Request<SummaryRequest>) -> Self::GetSummaryFuture {
        let SummaryRequest { index } = request.into_inner();
        if let Ok(idx) = self.catalog.read().get_index(&index) {
            if let Ok(metas) = idx.get_index().load_metas() {
                let meta_json = serde_json::to_vec(&metas).unwrap();
                Box::new(future::ok(Response::new(SummaryReply { summary: meta_json })))
            } else {
                Self::error_response(Code::DataLoss, format!("Could not load metas for: {}", index))
            }
        } else {
            Self::error_response(Code::NotFound, "Could not find index".into())
        }
    }

    fn bulk_insert(&mut self, _: Request<Streaming<BulkRequest>>) -> Self::BulkInsertFuture {
        unimplemented!()
    }

    fn ping(&mut self, _: Request<PingRequest>) -> Self::PingFuture {
        Box::new(future::ok(Response::new(PingReply { status: "OK".into() })))
    }
}

//#[cfg(test)]
//mod tests {
//    use future::Future;
//    use http::Uri;
//    use tokio::prelude::*;
//    use tokio::runtime::Runtime;
//
//    use toshi_test::get_localhost;
//
//    use crate::index::tests::create_test_catalog;
//
//    use super::*;
//    use failure::_core::time::Duration;
//
//    #[ignore]
//    fn rpc_test() {
//        std::env::set_var("RUST_LOG", "trace");
//        let sub = tracing_fmt::FmtSubscriber::builder()
//            .with_timer(tracing_fmt::time::SystemTime {})
//            .with_ansi(true)
//            .finish();
//        tracing::subscriber::set_global_default(sub).expect("Unable to set default Subscriber");
//
//        let catalog = create_test_catalog("test_index");
//        let addr = "127.0.0.1:8081".parse::<SocketAddr>().unwrap();
//        let router = RpcServer::serve(addr, Arc::clone(&catalog));
//        tokio::run(router);
//        //        let c = RpcServer::create_client("http://127.0.0.1:8081".parse::<Uri>().unwrap())
//        //            .map_err(|_| tower_grpc::Status::new(Code::DataLoss, ""))
//        //            .and_then(|mut client| {
//        //                client
//        //                    .list_indexes(tower_grpc::Request::new(ListRequest {}))
//        //                    .map(|resp| resp.into_inner())
//        //                    .inspect(|r: &ListReply| info!("{:?}aaaaaaaaaaaaaaaaaaaa", r))
//        //                    .map_err(Into::into)
//        //            })
//        //            .map(|_| ())
//        //            .map_err(|_| ());
//        //
//        //        tokio::run(router.join(c).map(|_| ()).map_err(|_| ()));
//        //        std::thread::sleep(Duration::from_millis(3000));
//    }
//}
