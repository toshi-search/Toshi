use std::net::SocketAddr;
use std::sync::Arc;

use slog::{info, Logger};
use tantivy::schema::Schema;
use tonic::{transport::Server, Code, Request, Response, Status};

use toshi_proto::cluster_rpc::*;
use toshi_types::{AddDocument, Catalog, DeleteDoc, DocsAffected, IndexHandle};

use crate::handle::RaftHandle;
use crate::rpc_utils::*;
use crate::BoxErr;

pub struct RpcServer<C, H>
where
    C: Catalog<Handle = RaftHandle<H>>,
    H: IndexHandle + Send + Sync + 'static,
{
    logger: Logger,
    catalog: Arc<C>,
}

impl<C, H> RpcServer<C, H>
where
    C: Catalog<Handle = RaftHandle<H>>,
    H: IndexHandle + Send + Sync + 'static,
{
    pub async fn serve(addr: SocketAddr, catalog: Arc<C>, logger: Logger) -> Result<(), BoxErr> {
        let service = server::IndexServiceServer::new(RpcServer {
            catalog,
            logger: logger.clone(),
        });

        Ok(Server::builder().add_service(service).serve(addr).await?)
    }
}

#[async_trait::async_trait]
impl<C, H> server::IndexService for RpcServer<C, H>
where
    C: Catalog<Handle = RaftHandle<H>>,
    H: IndexHandle + Send + Sync + 'static,
{
    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        Ok(Response::new(PingReply { status: "OK".into() }))
    }

    async fn place_index(&self, request: Request<PlaceRequest>) -> Result<Response<ResultReply>, Status> {
        let PlaceRequest { index, schema } = request.into_inner();
        let cat = Arc::clone(&self.catalog);
        if let Ok(schema) = serde_json::from_slice::<Schema>(&schema) {
            if cat.add_index(&index, schema).await.is_ok() {
                Ok(Response::new(ok_result()))
            } else {
                error_response(Code::Internal, format!("Insert: {} failed", index))
            }
        } else {
            error_response(Code::NotFound, "Invalid schema in request".into())
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
                    Ok(Response::new(ok_result()))
                } else {
                    error_response(Code::Internal, format!("Add Document Failed: {}", index))
                }
            } else {
                error_response(Code::Internal, format!("Invalid Document request: {}", index))
            }
        } else {
            error_response(Code::NotFound, "Could not find index".into())
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
                error_response(Code::Internal, format!("Invalid Document request: {}", index))
            }
        } else {
            error_response(Code::NotFound, "Could not find index".into())
        }
    }

    async fn search_index(&self, request: Request<SearchRequest>) -> Result<Response<SearchReply>, Status> {
        let inner = request.into_inner();
        let cat = Arc::clone(&self.catalog);
        {
            if let Ok(index) = cat.get_index(&inner.index) {
                let query = match query_or_all(&inner.query) {
                    Ok(v) => v,
                    Err(e) => return error_response(Code::Internal, e.to_string()),
                };
                info!(self.logger, "QUERY = {:?}", &query);

                match index.search_index(query).await {
                    Ok(query_results) => {
                        info!(self.logger, "Query Response = {:?}", query_results);
                        let query_bytes: Vec<u8> = serde_json::to_vec(&query_results).unwrap();
                        let result = Some(ok_result());
                        Ok(Response::new(create_search_reply(result, query_bytes)))
                    }
                    Err(e) => error_response(Code::Internal, e.to_string()),
                }
            } else {
                error_response(Code::NotFound, format!("Index: {} not found", inner.index))
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
                error_response(Code::DataLoss, format!("Could not load metas for: {}", index))
            }
        } else {
            error_response(Code::NotFound, "Could not find index".into())
        }
    }

    async fn raft_request(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let message: raft::eraftpb::Message = request.into_inner().message.unwrap();

        slog::debug!(self.logger, "MSG = {:?}", message);

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

#[cfg(test)]
pub mod tests {

    // #[allow(dead_code)]
    // pub fn create_test_catalog(name: &str) -> SharedCatalog {
    //     let idx = toshi_test::create_test_index();
    //     let catalog = IndexCatalog::with_index(name.into(), idx).unwrap();
    //     Arc::new(catalog)
    // }
    //
    // #[allow(dead_code)]
    // pub fn routes(port: i16) -> std::result::Result<(SocketAddr, Uri), Box<dyn std::error::Error>> {
    //     let addr = format!("127.0.0.1:{}", port).parse::<SocketAddr>()?;
    //     let uri = format!("http://127.0.0.1:{}/", port).parse::<Uri>()?;
    //     Ok((addr, uri))
    // }
}
