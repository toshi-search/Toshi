use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use http::Uri;
use prost::Message as _;
use slog::{info, Logger};
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::Index;

use tonic::transport::{self, Channel, Server};
use tonic::{Code, Request, Response, Status};

use toshi_proto::cluster_rpc::*;
use toshi_types::{AddDocument, Catalog, Error};
use toshi_types::{DeleteDoc, DocsAffected, IndexHandle, Search};

pub type RpcClient = client::IndexServiceClient<Channel>;

pub fn create_from_managed(mut base_path: PathBuf, index_path: &str, schema: Schema) -> Result<Index, Error> {
    base_path.push(index_path);
    if !base_path.exists() {
        fs::create_dir(&base_path)?;
    }
    let dir: MmapDirectory = MmapDirectory::open(base_path)?;
    Index::open_or_create(dir, schema).map_err(Into::into)
}

pub async fn create_client(uri: Uri, logger: Option<Logger>) -> Result<RpcClient, transport::Error> {
    if let Some(log) = logger {
        slog::info!(log, "Creating Client to: {:?}", uri);
    }
    client::IndexServiceClient::connect(uri.to_string()).await.map_err(Into::into)
}

pub struct RpcServer<C>
where
    C: Catalog,
{
    logger: Logger,
    catalog: Arc<C>,
}

impl<C> RpcServer<C>
where
    C: Catalog,
{
    pub async fn serve(
        addr: SocketAddr,
        catalog: Arc<C>,
        logger: Logger,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let service = server::IndexServiceServer::new(RpcServer {
            catalog,
            logger: logger.clone(),
        });

        Ok(Server::builder().add_service(service).serve(addr).await?)
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

    pub fn error_response<T>(code: Code, msg: String) -> Result<Response<T>, Status> {
        let status = Status::new(code, msg);
        Err(status)
    }

    pub fn query_or_all(b: &[u8]) -> Result<Search, Error> {
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
    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        Ok(Response::new(PingReply { status: "OK".into() }))
    }

    async fn place_index(&self, request: Request<PlaceRequest>) -> Result<Response<ResultReply>, Status> {
        let PlaceRequest { index, schema } = request.into_inner();
        let cat = Arc::clone(&self.catalog);
        if let Ok(schema) = serde_json::from_slice::<Schema>(&schema) {
            let ip = cat.base_path();
            if let Ok(new_index) = create_from_managed(ip.into(), &index, schema) {
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

    async fn list_indexes(&self, req: Request<ListRequest>) -> Result<Response<ListReply>, Status> {
        let cat = Arc::clone(&self.catalog);
        info!(self.logger, "Request From: {:?}", req);
        let indexes = cat.list_indexes().await;
        info!(self.logger, "Response: {:?}", indexes.join(", "));
        let resp = Response::new(ListReply { indexes });
        Ok(resp)
    }

    async fn place_document(&self, request: Request<DocumentRequest>) -> Result<Response<ResultReply>, Status> {
        info!(self.logger, "REQ = {:?}", &request);
        let DocumentRequest { index, document } = request.into_inner();
        let cat = Arc::clone(&self.catalog);
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

    async fn delete_document(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteReply>, Status> {
        let DeleteRequest { index, terms } = request.into_inner();
        let cat = Arc::clone(&self.catalog);
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

    async fn search_index(&self, request: Request<SearchRequest>) -> Result<Response<SearchReply>, Status> {
        let inner = request.into_inner();
        let cat = Arc::clone(&self.catalog);
        {
            if let Ok(index) = cat.get_index(&inner.index) {
                let query = match Self::query_or_all(&inner.query) {
                    Ok(v) => v,
                    Err(e) => return Self::error_response(Code::Internal, e.to_string()),
                };
                info!(self.logger, "QUERY = {:?}", &query);

                match index.search_index(query).await {
                    Ok(query_results) => {
                        info!(self.logger, "Query Response = {:?}", query_results);
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

    async fn get_summary(&self, request: Request<SummaryRequest>) -> Result<Response<SummaryReply>, Status> {
        let SummaryRequest { index } = request.into_inner();
        let cat = Arc::clone(&self.catalog);
        if let Ok(idx) = cat.get_index(&index) {
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

    async fn raft_request(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let RaftRequest { message, .. } = request.into_inner();
        let msg: toshi_proto::cluster_rpc::Message = Message::decode(Bytes::from(message)).unwrap();
        slog::debug!(self.logger, "MSG = {:?}", msg);

        let response = Response::new(RaftReply { code: 0 });
        Ok(response)
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<ResultReply>, Status> {
        let JoinRequest { host, id } = request.into_inner();
        let conf = raft::prelude::ConfChange {
            id,
            change_type: 0,
            node_id: id,
            context: host.as_bytes().to_vec(),
        };
        slog::debug!(self.logger, "CONF = {:?}", conf);

        let response = Response::new(ResultReply::default());
        Ok(response)
    }
}
