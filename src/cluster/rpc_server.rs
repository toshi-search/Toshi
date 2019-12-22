use std::net::SocketAddr;
use std::sync::Arc;

use tantivy::schema::Schema;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::*;

use toshi_proto::cluster_rpc::*;
use toshi_types::{DeleteDoc, DocsAffected, Search};

use crate::cluster::ConnectionError;
use crate::handle::IndexHandle;
use crate::index::IndexCatalog;
use crate::AddDocument;

pub type Buf = tonic::transport::Channel;
pub type RpcClient = client::IndexServiceClient<Buf>;

/// RPC Services should "ideally" work on only local indexes, they shouldn't be responsible for
/// going to other nodes to get index data. It should be the master's duty to know where the local
/// indexes are stored and make the RPC query to the node to get the data.
pub struct RpcServer {
    catalog: Arc<Mutex<IndexCatalog>>,
}

impl Clone for RpcServer {
    fn clone(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
        }
    }
}

impl RpcServer {
    pub async fn serve(addr: SocketAddr, catalog: Arc<Mutex<IndexCatalog>>) -> Result<(), tonic::transport::Error> {
        let service = server::IndexServiceServer::new(RpcServer { catalog });
        Server::builder().add_service(service).serve(addr).await
    }

    //TODO: Make DNS Threads and Buffer Requests Configurable options
    pub async fn create_client(uri: http::Uri) -> Result<RpcClient, ConnectionError> {
        info!("Creating Client to: {:?}", uri);
        client::IndexServiceClient::connect(uri.to_string()).await
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

    pub fn error_response<T>(code: Code, msg: String) -> Result<Response<T>, Status> {
        let status = Status::new(code, msg);
        Err(status)
    }
}

#[async_trait::async_trait]
impl server::IndexService for RpcServer {
    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        Ok(Response::new(PingReply { status: "OK".into() }))
    }

    async fn place_index(&self, request: Request<PlaceRequest>) -> Result<Response<ResultReply>, Status> {
        let PlaceRequest { index, schema } = request.into_inner();
        let mut cat = self.catalog.lock().await;
        if let Ok(schema) = serde_json::from_slice::<Schema>(&schema) {
            let ip = cat.base_path().clone();
            if let Ok(new_index) = IndexCatalog::create_from_managed(ip, &index.clone(), schema) {
                if cat.add_index(index.clone(), new_index).is_ok() {
                    Ok(Response::new(RpcServer::ok_result()))
                } else {
                    Self::error_response(Code::Internal, format!("Insert: {} failed", index))
                }
            } else {
                Self::error_response(Code::Internal, format!("Could not create index: {}", index))
            }
        } else {
            Self::error_response(Code::NotFound, "Invalid schema in request".into())
        }
    }

    async fn list_indexes(&self, req: Request<ListRequest>) -> Result<Response<ListReply>, Status> {
        let cat = self.catalog.lock().await;
        info!("Request From: {:?}", req);
        let indexes = cat.get_collection();
        let lists: Vec<String> = indexes.into_iter().map(|(t, _)| t.to_string()).collect();
        info!("Response: {:?}", lists.join(", "));
        let resp = Response::new(ListReply { indexes: lists });
        Ok(resp)
    }

    async fn place_document(&self, request: Request<DocumentRequest>) -> Result<Response<ResultReply>, Status> {
        let DocumentRequest { index, document } = request.into_inner();
        let cat = self.catalog.lock().await;
        if let Ok(idx) = cat.get_index(&index) {
            if let Ok(doc) = serde_json::from_slice::<AddDocument>(&document) {
                if idx.add_document(doc).await.is_ok() {
                    Ok(Response::new(RpcServer::ok_result()))
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

    async fn delete_document(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteReply>, Status> {
        let DeleteRequest { index, terms } = request.into_inner();
        let cat = self.catalog.lock().await;
        if let Ok(idx) = cat.get_index(&index) {
            if let Ok(delete_docs) = serde_json::from_slice::<DeleteDoc>(&terms) {
                let DocsAffected { docs_affected } = idx.delete_term(delete_docs).await?;
                Ok(Response::new(DeleteReply { index, docs_affected }))
            } else {
                Self::error_response(Code::Internal, format!("Invalid Document request: {}", index))
            }
        } else {
            Self::error_response(Code::NotFound, "Could not find index".into())
        }
    }

    async fn place_replica(&self, _request: Request<ReplicaRequest>) -> Result<Response<ResultReply>, Status> {
        unimplemented!()
    }

    async fn search_index(&self, request: Request<SearchRequest>) -> Result<Response<SearchReply>, Status> {
        let inner = request.into_inner();
        let cat = self.catalog.lock().await;
        {
            if let Ok(index) = cat.get_index(&inner.index) {
                let query: Search = match serde_json::from_slice(&inner.query) {
                    Ok(v) => v,
                    Err(e) => return Self::error_response(Code::Internal, e.to_string()),
                };
                info!("QUERY = {:?}", query);

                match index.search_index(query).await {
                    Ok(query_results) => {
                        info!("Query Response = {:?} hits", query_results.hits);
                        let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
                        let result = Some(RpcServer::ok_result());
                        Ok(Response::new(RpcServer::create_search_reply(result, query_bytes)))
                    }
                    Err(e) => Self::error_response(Code::Internal, e.to_string()),
                }
            } else {
                Self::error_response(Code::NotFound, format!("Index: {} not found", inner.index))
            }
        }
    }

    async fn get_summary(&self, request: Request<SummaryRequest>) -> Result<Response<SummaryReply>, Status> {
        let SummaryRequest { index } = request.into_inner();
        if let Ok(idx) = self.catalog.lock().await.get_index(&index) {
            if let Ok(metas) = idx.get_index().load_metas() {
                let meta_json = serde_json::to_vec(&metas).unwrap();
                Ok(Response::new(SummaryReply { summary: meta_json }))
            } else {
                Self::error_response(Code::DataLoss, format!("Could not load metas for: {}", index))
            }
        } else {
            Self::error_response(Code::NotFound, "Could not find index".into())
        }
    }

    async fn bulk_insert(&self, _: Request<Streaming<BulkRequest>>) -> Result<Response<ResultReply>, Status> {
        unimplemented!()
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
