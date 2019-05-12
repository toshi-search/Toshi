use std::collections::HashMap;
use std::convert::Into;
use std::sync::{Arc, RwLock};

use futures::future::Either;
use futures::stream::{futures_unordered, Stream};
use futures::{future, Future};
use http::{StatusCode, Response};
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
use crate::index::IndexCatalog;
use crate::router::empty_with_code;

#[derive(Deserialize, Clone)]
pub struct SchemaBody(pub Schema);

#[derive(Debug, Deserialize)]
pub struct DeleteDoc {
    pub options: Option<IndexOptions>,
    pub terms: HashMap<String, String>,
}

#[derive(Clone)]
pub struct IndexHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

#[derive(Debug, Deserialize)]
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
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        IndexHandler { catalog }
    }

    fn add_index(catalog: &Arc<RwLock<IndexCatalog>>, name: String, index: Index) -> Result<(), Error> {
        match catalog.write() {
            Ok(ref mut cat) => cat.add_index(name, index),
            Err(e) => Err(Error::IOError(e.to_string())),
        }
    }

    fn add_remote_index(catalog: &Arc<RwLock<IndexCatalog>>, name: String, clients: Vec<RpcClient>) -> Result<(), Error> {
        match catalog.write() {
            Ok(ref mut cat) => cat.add_multi_remote_index(name, clients),
            Err(e) => Err(Error::IOError(e.to_string())),
        }
    }

    fn delete_term(catalog: &Arc<RwLock<IndexCatalog>>, body: DeleteDoc, index: String) -> Result<DocsAffected, Error> {
        if let Ok(mut index_lock) = catalog.write() {
            if index_lock.exists(&index) {
                let index_handle = index_lock.get_mut_index(&index)?;
                index_handle.delete_term(body)
            } else {
                Err(Error::IOError("Index does not exist".into()))
            }
        } else {
            Err(Error::IOError("Failed to obtain lock on catalog".into()))
        }
    }

    fn create_remote_index(
        nodes: Vec<String>,
        index: String,
        schema: Schema,
    ) -> impl Stream<Item = Vec<RpcClient>, Error = RPCError> + Send {
        let futs = nodes.into_iter().map(move |n| {
            let c = IndexCatalog::create_client(n.clone());
            let index = index.clone();
            let schema = schema.clone();
            c.and_then(move |mut client| {
                let client_clone = client.clone();
                let s = serde_json::to_vec(&schema).unwrap();
                let request = Request::new(PlaceRequest {
                    index: index.clone(),
                    schema: s,
                });
                client.place_index(request).map(move |_| vec![client_clone]).map_err(Into::into)
            })
        });
        futures_unordered(futs)
    }

    pub fn create_index(&self, body: Body, index: String) -> ResponseFuture {
        let cat_clone = Arc::clone(&self.catalog);
        let idx_clone = index.clone();
        let nodes = cat_clone.read().unwrap().settings.experimental_features.nodes.clone();

        let task = body
            .concat2()
            .map_err(|e: hyper::Error| -> Error { e.into() })
            .map(|b| serde_json::from_slice::<SchemaBody>(&b).unwrap())
            .and_then(move |b| {
                {
                    let base_path = cat_clone.read().unwrap().base_path().clone();
                    let new_index: Index = match IndexCatalog::create_from_managed(base_path, &index, b.0.clone()) {
                        Ok(v) => v,
                        Err(e) => return Either::A(future::ok(Response::from(e)))
                    };
                    match IndexHandler::add_index(&cat_clone, index.clone(), new_index) {
                        Ok(_) => (),
                        Err(e) => return Either::A(future::ok(Response::from(e)))
                    };
                }

                Either::B(IndexHandler::create_remote_index(nodes, index, b.0)
                    .concat2()
                    .map(move |clients| {
                        IndexHandler::add_remote_index(&cat_clone, idx_clone, clients)
                            .map(|()| empty_with_code(StatusCode::CREATED))
                            .unwrap()
                    })
                    .map_err(Into::into))
            })
            .map_err(failure::Error::from);
        Box::new(task)
    }

    pub fn add_document(&self, body: Body, index: String) -> ResponseFuture {
        let cat_clone = Arc::clone(&self.catalog);
        let task = body
            .concat2()
            .map_err(|e: hyper::Error| -> Error { e.into() })
            .map(|b| serde_json::from_slice::<AddDocument>(&b).unwrap())
            .and_then(move |b| match cat_clone.read() {
                Ok(cat) => {
                    let location: bool = random();
                    if location && cat.remote_exists(&index) {
                        Either::A(
                            cat.add_remote_document(&index, b.clone())
                                .map(|_| empty_with_code(StatusCode::CREATED))
                                .map_err(Into::into),
                        )
                    } else {
                        Either::B(
                            cat.add_local_document(&index, b.clone())
                                .map(|_| empty_with_code(StatusCode::CREATED))
                                .map_err(Into::into),
                        )
                    }
                }
                _ => panic!(":("),
            })
            .map_err(failure::Error::from);
        Box::new(task)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use tokio::prelude::*;

    use crate::handlers::SearchHandler;
    use crate::index::tests::*;

    use super::*;

    #[test]
    fn test_create_index() -> ::std::io::Result<()> {
        let shared_cat = create_test_catalog("test_index");
        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_unindex", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;
        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let body: SchemaBody = serde_json::from_str(schema).unwrap();
        //        handler.create(body, "new_index".into()).wait().unwrap();
        let search = SearchHandler::new(Arc::clone(&shared_cat));
        let docs = search.all_docs("new_index".into()).wait();

        assert_eq!(docs.is_ok(), true);
        //        assert_eq!(docs.unwrap().hits, 0);
        remove_dir_all::remove_dir_all("new_index")
    }

    #[test]
    fn test_doc_create() {
        let shared_cat = create_test_catalog("test_index");
        let body: AddDocument = serde_json::from_str(
            r#" {"options": {"commit": true }, "document": {"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10} }"#,
        )
        .unwrap();

        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        //        let req = handler.add(body, "test_index".into()).wait();
        //
        //        assert_eq!(req.is_ok(), true);
    }

    #[test]
    fn test_doc_delete() {
        let shared_cat = create_test_catalog("test_index");
        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let mut terms = HashMap::new();
        terms.insert("test_text".to_string(), "document".to_string());
        let delete = DeleteDoc {
            options: Some(IndexOptions { commit: true }),
            terms,
        };
        //        let req = handler.delete(delete, "test_index".into());
        //
        //        assert_eq!(req.is_ok(), true);
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
        //        let req = handler.add(add_doc, "test_index".into()).wait();
        //        assert_eq!(req.is_err(), true);
    }
}
