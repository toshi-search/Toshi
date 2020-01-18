use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use http::Uri;
use prost::Message as _;

use raft::Config;
use slog::*;
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::Index;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status, Streaming};

use toshi_proto::cluster_rpc::*;
use toshi_types::{AddDocument, Catalog};
use toshi_types::{DeleteDoc, DocsAffected, IndexHandle, Search};

use crate::raft_node::ToshiRaft;

pub type RPCResult<R> = std::result::Result<R, Status>;
pub type RpcClient = client::IndexServiceClient<tonic::transport::Channel>;

pub fn create_from_managed(mut base_path: PathBuf, index_path: &str, schema: Schema) -> std::result::Result<Index, toshi_types::Error> {
    base_path.push(index_path);
    if !base_path.exists() {
        fs::create_dir(&base_path).map_err(|e| toshi_types::Error::IOError(e.to_string()))?;
    }
    let dir = MmapDirectory::open(base_path).map_err(|e| toshi_types::Error::IOError(e.to_string()))?;
    Index::open_or_create(dir, schema).map_err(|e| toshi_types::Error::IOError(e.to_string()))
}

pub struct RpcServer<C>
where
    C: Catalog,
{
    logger: Logger,
    raft: Arc<Mutex<ToshiRaft>>,
    catalog: Arc<Mutex<C>>,
}

impl<C> RpcServer<C>
where
    C: Catalog,
{
    pub async fn serve(addr: SocketAddr, catalog: Arc<Mutex<C>>, logger: Logger) -> std::result::Result<(), tonic::transport::Error> {
        let raft = Arc::new(Mutex::new(ToshiRaft::new(&Config::new(1), &logger).unwrap()));
        let l = Arc::clone(&raft);
        tokio::spawn(async move {
            let mut l = l.lock().await;
            l.run().await
        });
        let service = server::IndexServiceServer::new(RpcServer { catalog, logger, raft });

        Server::builder().add_service(service).serve(addr).await
    }

    //TODO: Make DNS Threads and Buffer Requests Configurable options
    pub async fn create_client(uri: Uri, logger: Option<Logger>) -> std::result::Result<RpcClient, tonic::transport::Error> {
        if let Some(log) = logger {
            info!(log, "Creating Client to: {:?}", uri);
        }
        client::IndexServiceClient::connect(uri.to_string()).await.map_err(Into::into)
    }

    pub fn ok_result() -> ResultReply {
        Self::create_result(0, "".into())
    }

    pub fn create_result(code: i32, message: String) -> ResultReply {
        ResultReply { code, message }
    }

    pub fn create_search_reply(result: Option<ResultReply>, doc: Vec<u8>) -> SearchReply {
        SearchReply { result, doc }
    }

    pub fn error_response<T>(code: Code, msg: String) -> RPCResult<Response<T>> {
        let status = Status::new(code, msg);
        Err(status)
    }

    pub fn query_or_all(b: &[u8]) -> std::result::Result<Search, toshi_types::Error> {
        let deser: Search = serde_json::from_slice(b)?;
        if deser.query.is_none() {
            return Ok(Search::all_docs());
        }
        Ok(deser)
    }
}

#[async_trait::async_trait]
impl<C> server::IndexService for RpcServer<C>
where
    C: Catalog,
{
    async fn ping(&self, _: Request<PingRequest>) -> std::result::Result<Response<PingReply>, Status> {
        Ok(Response::new(PingReply { status: "OK".into() }))
    }

    async fn place_index(&self, request: Request<PlaceRequest>) -> RPCResult<Response<ResultReply>> {
        let PlaceRequest { index, schema } = request.into_inner();
        let mut cat = self.catalog.lock().await;
        if let Ok(schema) = serde_json::from_slice::<Schema>(&schema) {
            let ip = cat.base_path();
            if let Ok(new_index) = create_from_managed(ip.into(), &index.clone(), schema) {
                if cat.add_index(index.clone(), new_index).is_ok() {
                    Ok(Response::new(Self::ok_result()))
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

    async fn list_indexes(&self, req: Request<ListRequest>) -> RPCResult<Response<ListReply>> {
        let cat = self.catalog.lock().await;
        info!(self.logger, "Request From: {:?}", req);
        let indexes = cat.list_indexes().await;
        info!(self.logger, "Response: {:?}", indexes.join(", "));
        let resp = Response::new(ListReply { indexes });
        Ok(resp)
    }

    async fn place_document(&self, request: Request<DocumentRequest>) -> RPCResult<Response<ResultReply>> {
        info!(self.logger, "REQ = {:?}", &request);
        let DocumentRequest { index, document } = request.into_inner();
        let cat = self.catalog.lock().await;
        if let Ok(idx) = cat.get_index(&index) {
            if let Ok(doc) = serde_json::from_slice::<AddDocument<serde_json::Value>>(&document) {
                if idx.add_document(doc).await.is_ok() {
                    Ok(Response::new(RpcServer::<C>::ok_result()))
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

    async fn delete_document(&self, request: Request<DeleteRequest>) -> RPCResult<Response<DeleteReply>> {
        let DeleteRequest { index, terms } = request.into_inner();
        let cat = self.catalog.lock().await;
        if let Ok(idx) = cat.get_index(&index) {
            if let Ok(delete_docs) = serde_json::from_slice::<DeleteDoc>(&terms) {
                let DocsAffected { docs_affected } = idx.delete_term(delete_docs).await.unwrap();
                Ok(Response::new(DeleteReply { index, docs_affected }))
            } else {
                Self::error_response(Code::Internal, format!("Invalid Document request: {}", index))
            }
        } else {
            Self::error_response(Code::NotFound, "Could not find index".into())
        }
    }

    async fn search_index(&self, request: Request<SearchRequest>) -> RPCResult<Response<SearchReply>> {
        let inner = request.into_inner();
        let cat = self.catalog.lock().await;
        {
            if let Ok(index) = cat.get_index(&inner.index) {
                let query = match Self::query_or_all(&inner.query) {
                    Ok(v) => v,
                    Err(e) => return Self::error_response(Code::Internal, e.to_string()),
                };
                info!(self.logger, "QUERY = {:?}", &query);

                match index.search_index(query).await {
                    Ok(query_results) => {
                        info!(self.logger, "Query Response = {:?} hits", query_results.hits);
                        let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
                        let result = Some(RpcServer::<C>::ok_result());
                        Ok(Response::new(RpcServer::<C>::create_search_reply(result, query_bytes)))
                    }
                    Err(e) => Self::error_response(Code::Internal, e.to_string()),
                }
            } else {
                Self::error_response(Code::NotFound, format!("Index: {} not found", inner.index))
            }
        }
    }

    async fn get_summary(&self, request: Request<SummaryRequest>) -> RPCResult<Response<SummaryReply>> {
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

    async fn bulk_insert(&self, _: Request<Streaming<BulkRequest>>) -> RPCResult<Response<ResultReply>> {
        unimplemented!()
    }

    async fn raft_request(&self, request: Request<RaftRequest>) -> RPCResult<Response<RaftReply>> {
        let RaftRequest { message, .. } = request.into_inner();
        let msg: toshi_proto::cluster_rpc::Message = Message::decode(Bytes::from(message)).unwrap();
        let l = Arc::clone(&self.raft);
        slog::info!(self.logger, "MSG = {:?}", msg);

        tokio::spawn(async move { l.lock().await.send(msg.into()).await.unwrap() });
        let response = Response::new(RaftReply { code: 0 });
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use futures::future::{try_select, Either};
    use http::Uri;

    use super::*;
    use toshi_server::index::{IndexCatalog, SharedCatalog};

    pub fn create_test_catalog(name: &str) -> SharedCatalog {
        let idx = toshi_test::create_test_index();
        let catalog = IndexCatalog::with_index(name.into(), idx).unwrap();
        Arc::new(Mutex::new(catalog))
    }

    pub fn routes(port: i16) -> std::result::Result<(SocketAddr, Uri), Box<dyn std::error::Error>> {
        let addr = format!("127.0.0.1:{}", port).parse::<SocketAddr>()?;
        let uri = format!("http://127.0.0.1:{}/", port).parse::<Uri>()?;
        Ok((addr, uri))
    }

    //    #[ignore]
    #[tokio::test(threaded_scheduler)]
    async fn rpc_ping() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");

        let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let drain = slog_term::FullFormat::new(decorator)
            .use_original_order()
            .use_local_timestamp()
            .build()
            .fuse();
        let async_drain = slog_async::Async::new(drain).build().fuse();
        let root_log = slog::Logger::root(async_drain, o!("toshi" => "toshi"));

        let (addr, uri) = routes(8079)?;
        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog), root_log.clone()));
        let mut client = RpcServer::<IndexCatalog>::create_client(uri, Some(root_log.clone())).await?;
        let mut msg = Message::default();
        msg.msg_type = 2;
        msg.term = 1;
        msg.index = 1;
        msg.commit = 1;
        msg.to = 1;
        msg.from = 1;
        let mut entry = Entry::default();
        msg.context = br#"test_index"#.to_vec();
        entry.context = br#"test_index"#.to_vec();
        entry.data = br#"{"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10}"#.to_vec();
        msg.entries = vec![entry.into()];

        let mut msg_bytes = vec![];
        msg.encode(&mut msg_bytes)?;
        let req = RaftRequest {
            message: msg_bytes,
            tpe: 0,
        };
        std::thread::sleep(std::time::Duration::from_secs(2));

        let list = tokio::spawn(async move { client.raft_request(Request::new(req)).await });

        let sel: RaftReply = match try_select(list, router).await.unwrap() {
            Either::Left((Ok(v), _)) => v.into_inner(),
            e => panic!("{:?}", e),
        };

        std::thread::sleep(std::time::Duration::from_secs(2));

        println!("{:?}", sel);

        Ok(())
    }
}
//
//    #[ignore]
//    #[tokio::test]
//    async fn rpc_list() -> Result<(), Box<dyn std::error::Error>> {
//        let catalog = create_test_catalog("test_index");
//        let (addr, uri) = routes(8081)?;
//        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog)));
//        let mut client = RpcServer::create_client(uri).await?;
//        let list = tokio::spawn(async move { client.list_indexes(Request::new(ListRequest {})).await });
//        let sel: ListReply = match try_select(list, router).await.unwrap() {
//            Either::Left((Ok(v), _)) => v.into_inner(),
//            _ => unreachable!(),
//        };
//        assert_eq!(sel.indexes.len(), 1);
//        assert_eq!(sel.indexes[0], "test_index");
//        Ok(())
//    }
//
//    #[ignore]
//    #[tokio::test]
//    async fn rpc_summary() -> Result<(), Box<dyn std::error::Error>> {
//        let catalog = create_test_catalog("test_index");
//        let (addr, uri) = routes(8082)?;
//        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog)));
//        let mut client = RpcServer::create_client(uri).await?;
//        let list = tokio::spawn(async move {
//            client
//                .get_summary(Request::new(SummaryRequest {
//                    index: "test_index".into(),
//                }))
//                .await
//        });
//        let sel: SummaryReply = match try_select(list, router).await.unwrap() {
//            Either::Left((Ok(v), _)) => v.into_inner(),
//            e => panic!("{:?}", e),
//        };
//        assert_eq!(sel.summary.is_empty(), false);
//        Ok(())
//    }
//
//    #[ignore]
//    #[tokio::test]
//    async fn rpc_search() -> Result<(), Box<dyn std::error::Error>> {
//        let catalog = create_test_catalog("test_index");
//        let (addr, uri) = routes(8083)?;
//        let router = tokio::spawn(RpcServer::serve(addr, Arc::clone(&catalog)));
//        let mut client = RpcServer::create_client(uri).await?;
//        let query = Search::all_docs();
//        let query_bytes = serde_json::to_vec(&query)?;
//        let list = tokio::spawn(async move {
//            client
//                .search_index(Request::new(SearchRequest {
//                    index: "test_index".into(),
//                    query: query_bytes,
//                }))
//                .await
//        });
//        let sel: SearchReply = match try_select(list, router).await.unwrap() {
//            Either::Left((Ok(v), _)) => v.into_inner(),
//            e => panic!("{:?}", e),
//        };
//        let results: crate::SearchResults = serde_json::from_slice(&sel.doc)?;
//        assert_eq!(results.hits, 5);
//        Ok(())
//    }
//}
