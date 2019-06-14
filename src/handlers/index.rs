use std::collections::HashMap;
use std::convert::Into;
use std::sync::Arc;

use futures::stream::{futures_unordered, Stream};
use futures::{future, future::Either, Future};
use http::{Response, StatusCode};
use hyper::Body;
use rand::random;
use serde::{Deserialize, Serialize};
use tantivy::schema::*;
use tantivy::Index;
use tower_grpc::Request;

use toshi_proto::cluster_rpc::PlaceRequest;

use crate::cluster::rpc_server::RpcClient;
use crate::cluster::RPCError;
use crate::error::Error;
use crate::handle::IndexHandle;
use crate::handlers::ResponseFuture;
use crate::index::{IndexCatalog, SharedCatalog};
use crate::utils::{empty_with_code, error_response, with_body};

#[derive(Deserialize, Clone)]
pub struct SchemaBody(pub Schema);

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteDoc {
    pub options: Option<IndexOptions>,
    pub terms: HashMap<String, String>,
}

#[derive(Clone)]
pub struct IndexHandler {
    catalog: SharedCatalog,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DocsAffected {
    pub docs_affected: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexOptions {
    #[serde(default)]
    pub commit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddDocument {
    pub options: Option<IndexOptions>,
    pub document: serde_json::Value,
}

impl IndexHandler {
    pub fn new(catalog: SharedCatalog) -> Self {
        IndexHandler { catalog }
    }

    #[inline]
    fn add_index(catalog: SharedCatalog, name: String, index: Index) -> Result<(), Error> {
        catalog.write().add_index(name, index)
    }

    #[inline]
    fn add_remote_index(catalog: SharedCatalog, name: String, clients: Vec<RpcClient>) -> Result<(), Error> {
        catalog.write().add_multi_remote_index(name, clients)
    }

    fn delete_terms(catalog: SharedCatalog, body: DeleteDoc, index: &str) -> Result<DocsAffected, Error> {
        let index_lock = catalog.read();
        let index_handle = index_lock.get_index(index)?;
        index_handle.delete_term(body)
    }

    fn create_remote_index(nodes: &[String], index: String, schema: Schema) -> impl Stream<Item = Vec<RpcClient>, Error = RPCError> + Send {
        let futs = nodes.iter().map(move |n| {
            let c = IndexCatalog::create_client(n.clone());
            let index = index.clone();
            let schema = schema.clone();
            c.and_then(move |mut client| {
                let client_clone = client.clone();
                let schema_bytes = serde_json::to_vec(&schema).unwrap();
                let request = Request::new(PlaceRequest {
                    index: index.clone(),
                    schema: schema_bytes,
                });
                client.place_index(request).map(move |_| vec![client_clone]).map_err(Into::into)
            })
        });
        futures_unordered(futs)
    }

    pub fn delete_term(&self, body: Body, index: String) -> ResponseFuture {
        let cat = Arc::clone(&self.catalog);
        let fut = body
            .concat2()
            .and_then(move |b| {
                let b = match serde_json::from_slice::<DeleteDoc>(&b) {
                    Ok(v) => v,
                    Err(e) => return future::Either::A(future::ok(Response::from(Error::from(e)))),
                };
                let docs_affected = match IndexHandler::delete_terms(cat, b, &index) {
                    Ok(v) => with_body(v),
                    Err(e) => return future::Either::A(future::ok(Response::from(e))),
                };

                future::Either::B(future::ok(docs_affected))
            })
            .or_else(|_| future::ok(empty_with_code(StatusCode::INTERNAL_SERVER_ERROR)));

        Box::new(fut)
    }

    pub fn create_index(&self, body: Body, index: String) -> ResponseFuture {
        let cat = Arc::clone(&self.catalog);
        Box::new(body.concat2().and_then(move |b| {
            let b = match serde_json::from_slice::<SchemaBody>(&b) {
                Ok(v) => v,
                Err(e) => return future::Either::A(future::ok(Response::from(Error::from(e)))),
            };

            {
                let base_path = cat.read().base_path().clone();
                let new_index: Index = match IndexCatalog::create_from_managed(base_path, &index, b.0.clone()) {
                    Ok(v) => v,
                    Err(e) => return future::Either::A(future::ok(Response::from(e))),
                };
                match IndexHandler::add_index(Arc::clone(&cat), index.clone(), new_index) {
                    Ok(_) => (),
                    Err(e) => return future::Either::A(future::ok(Response::from(e))),
                };
            }

            let expir = cat.read().settings.experimental;
            if expir {
                let nodes = &cat.read().settings.get_nodes();
                future::Either::B(
                    IndexHandler::create_remote_index(&nodes, index.clone(), b.0)
                        .concat2()
                        .map(move |clients| {
                            IndexHandler::add_remote_index(cat, index, clients)
                                .map(|()| empty_with_code(StatusCode::CREATED))
                                .unwrap()
                        })
                        .or_else(|_| future::ok(empty_with_code(StatusCode::INTERNAL_SERVER_ERROR))),
                )
            } else {
                future::Either::A(future::ok(empty_with_code(StatusCode::CREATED)))
            }
        }))
    }

    pub fn add_document(&self, body: Body, index: String) -> ResponseFuture {
        let cat_clone = Arc::clone(&self.catalog);
        let task = body.concat2().and_then(move |b| {
            let b = serde_json::from_slice::<AddDocument>(&b).unwrap();
            let cat = cat_clone.read();
            let location: bool = random();
            if location && cat.remote_exists(&index) {
                let t = cat
                    .add_remote_document(&index, b.clone())
                    .map(|_| empty_with_code(StatusCode::CREATED))
                    .or_else(|e| future::ok(error_response(StatusCode::BAD_REQUEST, e)));

                Either::A(t)
            } else {
                let t = cat
                    .add_local_document(&index, b.clone())
                    .map(|_| empty_with_code(StatusCode::CREATED))
                    .or_else(|e| future::ok(error_response(StatusCode::BAD_REQUEST, e)));

                Either::B(t)
            }
        });

        Box::new(task)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use tokio::prelude::*;

    use crate::handlers::SearchHandler;
    use crate::index::tests::*;
    use crate::results::SearchResults;

    use super::*;

    fn test_index() -> String {
        String::from("test_index")
    }

    #[test]
    fn test_create_index() {
        let shared_cat = create_test_catalog("test_index");
        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_unindex", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;
        let handler = IndexHandler::new(Arc::clone(&shared_cat));

        handler.create_index(Body::from(schema), "new_index".into()).wait().unwrap();
        let search = SearchHandler::new(Arc::clone(&shared_cat));
        let docs = search
            .all_docs("new_index".into())
            .wait()
            .unwrap()
            .into_body()
            .concat2()
            .wait()
            .unwrap();
        let body: SearchResults = serde_json::from_slice(&docs).unwrap();

        assert_eq!(body.hits, 0);
        remove_dir_all::remove_dir_all("new_index").unwrap();
    }

    #[test]
    fn test_doc_create() {
        let shared_cat = create_test_catalog("test_index");
        let body = r#" {"options": {"commit": true }, "document": {"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10} }"#;
        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let req = handler.add_document(Body::from(body), test_index()).wait();

        assert_eq!(req.is_ok(), true);
    }

    #[test]
    fn test_doc_delete() {
        let shared_cat = create_test_catalog("test_index");
        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let mut terms = HashMap::new();
        terms.insert(test_index(), "document".to_string());
        let delete = DeleteDoc {
            options: Some(IndexOptions { commit: true }),
            terms,
        };
        let body_bytes = serde_json::to_vec(&delete).unwrap();
        let req = handler.delete_term(Body::from(body_bytes), test_index()).wait();
        assert_eq!(req.is_ok(), true);
    }

    #[test]
    fn test_bad_json() {
        let shared_cat = create_test_catalog("test_index");
        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let bad_json: serde_json::Value = serde_json::Value::String("".into());
        let add_doc = AddDocument {
            document: bad_json,
            options: None,
        };
        let body_bytes = serde_json::to_vec(&add_doc).unwrap();
        let req = handler
            .add_document(Body::from(body_bytes), test_index())
            .wait()
            .unwrap()
            .into_body()
            .concat2()
            .wait()
            .unwrap();
        println!("{}", std::str::from_utf8(&req).unwrap());
    }
}
