use std::net::SocketAddr;
use std::sync::Arc;

use tantivy::schema::Schema;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::*;

use toshi_proto::cluster_rpc::*;
use toshi_types::{DeleteDoc, DocsAffected, Error, Search};

use crate::handle::IndexHandle;
use crate::index::IndexCatalog;
use crate::AddDocument;

pub type Buf = tonic::transport::Channel;
pub type RpcClient = client::IndexServiceClient<Buf>;

pub struct RpcServer {
    catalog: Arc<Mutex<IndexCatalog>>,
}

impl RpcServer {
    pub async fn serve(addr: SocketAddr, catalog: Arc<Mutex<IndexCatalog>>) -> Result<(), tonic::transport::Error> {
        let service = server::IndexServiceServer::new(RpcServer { catalog });
        Server::builder().add_service(service).serve(addr).await
    }

    //TODO: Make DNS Threads and Buffer Requests Configurable options
    pub async fn create_client(uri: http::Uri) -> Result<RpcClient, Error> {
        info!("Creating Client to: {:?}", uri);
        client::IndexServiceClient::connect(uri.to_string()).await.map_err(Into::into)
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

    pub fn query_or_all(b: &[u8]) -> Result<Search, Box<dyn std::error::Error>> {
        let deser: Search = serde_json::from_slice(b)?;
        if deser.query.is_none() {
            return Ok(Search::all_docs());
        }
        Ok(deser)
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
        info!("REQ = {:?}", &request);
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

    async fn search_index(&self, request: Request<SearchRequest>) -> Result<Response<SearchReply>, Status> {
        let inner = request.into_inner();
        let cat = self.catalog.lock().await;
        {
            if let Ok(index) = cat.get_index(&inner.index) {
                let query = match Self::query_or_all(&inner.query) {
                    Ok(v) => v,
                    Err(e) => return Self::error_response(Code::Internal, e.to_string()),
                };
                info!("QUERY = {:?}", &query);

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

#[cfg(test)]
mod tests {
    use futures::future::{try_select, Either};
    use http::Uri;

    use crate::index::tests::create_test_catalog;

    use super::*;

    pub fn routes(port: i16) -> Result<(SocketAddr, Uri), Box<dyn std::error::Error>> {
        let addr = format!("127.0.0.1:{}", port).parse::<SocketAddr>()?;
        let uri = format!("http://127.0.0.1:{}/", port).parse::<Uri>()?;
        Ok((addr, uri))
    }

    #[ignore]
    #[tokio::test]
    async fn rpc_ping() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let (addr, uri) = routes(8079)?;
        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog)));
        let mut client = RpcServer::create_client(uri).await?;
        let list = tokio::spawn(async move { client.ping(Request::new(PingRequest {})).await });
        let sel: PingReply = match try_select(list, router).await.unwrap() {
            Either::Left((Ok(v), _)) => v.into_inner(),
            e => panic!("{:?}", e),
        };
        assert_eq!(sel.status, "OK");
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn rpc_list() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let (addr, uri) = routes(8081)?;
        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog)));
        let mut client = RpcServer::create_client(uri).await?;
        let list = tokio::spawn(async move { client.list_indexes(Request::new(ListRequest {})).await });
        let sel: ListReply = match try_select(list, router).await.unwrap() {
            Either::Left((Ok(v), _)) => v.into_inner(),
            _ => unreachable!(),
        };
        assert_eq!(sel.indexes.len(), 1);
        assert_eq!(sel.indexes[0], "test_index");
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn rpc_summary() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let (addr, uri) = routes(8082)?;
        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog)));
        let mut client = RpcServer::create_client(uri).await?;
        let list = tokio::spawn(async move {
            client
                .get_summary(Request::new(SummaryRequest {
                    index: "test_index".into(),
                }))
                .await
        });
        let sel: SummaryReply = match try_select(list, router).await.unwrap() {
            Either::Left((Ok(v), _)) => v.into_inner(),
            e => panic!("{:?}", e),
        };
        assert_eq!(sel.summary.is_empty(), false);
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn rpc_search() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let (addr, uri) = routes(8083)?;
        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog)));
        let mut client = RpcServer::create_client(uri).await?;
        let query = Search::all_docs();
        let query_bytes = serde_json::to_vec(&query)?;
        let list = tokio::spawn(async move {
            client
                .search_index(Request::new(SearchRequest {
                    index: "test_index".into(),
                    query: query_bytes,
                }))
                .await
        });
        let sel: SearchReply = match try_select(list, router).await.unwrap() {
            Either::Left((Ok(v), _)) => v.into_inner(),
            e => panic!("{:?}", e),
        };
        let results: crate::SearchResults = serde_json::from_slice(&sel.doc)?;
        assert_eq!(results.hits, 5);
        Ok(())
    }
}
