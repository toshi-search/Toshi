use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::future::{Either, Future};
use futures::stream::{futures_unordered, Stream};
use serde::{Deserialize, Serialize};
use tantivy::schema::*;
use tantivy::Index;
use tower_grpc::Request;
use tower_web::*;

use toshi_proto::cluster_rpc::*;

use crate::cluster::rpc_server::RpcClient;
use crate::cluster::RPCError;
use crate::error::Error;
use crate::handle::IndexHandle;
use crate::handlers::CreatedResponse;
use crate::index::IndexCatalog;
use futures::stream::*;
use futures::Future;
use toshi_proto::cluster_rpc::*;
use tower_grpc::Request;

#[derive(Extract, Deserialize)]
pub struct SchemaBody(pub Schema);

#[derive(Debug, Extract, Deserialize)]
pub struct DeleteDoc {
    pub options: Option<IndexOptions>,
    pub terms: HashMap<String, String>,
}

#[derive(Clone)]
pub struct IndexHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

#[derive(Debug, Response, Deserialize)]
pub struct DocsAffected {
    pub docs_affected: u64,
}

#[derive(Debug, Clone, Extract, Serialize, Deserialize)]
pub struct IndexOptions {
    #[serde(default)]
    pub commit: bool,
}

#[derive(Debug, Clone, Extract, Serialize, Deserialize)]
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

    fn create_remote_index(&self, index: String, schema: Schema) -> impl Stream<Item = Vec<RpcClient>, Error = RPCError> + Send {
        let nodes = self.catalog.read().unwrap().settings.experimental_features.nodes.clone();
        dbg!(&nodes);
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
                client.place_index(request).map(move |_| vec![client_clone]).map_err(|e| e.into())
            })
        });
        futures_unordered(futs)
    }

    fn inner_create(&self, body: SchemaBody, index: String) -> impl Future<Item = CreatedResponse, Error = Error> + Send {
        let cat_clone = Arc::clone(&self.catalog);
        let idx_clone = index.clone();
        {
            let base_path = cat_clone.try_read().unwrap().base_path().clone();
            let new_index = IndexCatalog::create_from_managed(base_path, &index, body.0.clone()).unwrap();
            IndexHandler::add_index(&cat_clone, index.clone(), new_index).unwrap();
        }
        self.create_remote_index(index, body.0)
            .concat2()
            .map(move |clients| {
                IndexHandler::add_remote_index(&cat_clone, idx_clone, clients).unwrap();
                CreatedResponse
            })
            .map_err(|e| e.into())
    }

    fn inner_add(&self, body: AddDocument, index: String) -> impl Future<Item = CreatedResponse, Error = Error> + Send {
        match self.catalog.write() {
            Ok(cat) => {
                let tasks = vec![
                    Either::A(cat.add_local_document(&index, body.clone())),
                    Either::B(cat.add_remote_document(&index, body.clone())),
                ];

                futures_unordered(tasks).collect().map(|_| CreatedResponse)
            }
            Err(_) => panic!(":("),
        }
    }
}

impl_web! {
    impl IndexHandler {
        #[delete("/:index")]
        #[content_type("application/json")]
        pub fn delete(&self, body: DeleteDoc, index: String) -> Result<DocsAffected, Error> {
            IndexHandler::delete_term(&self.catalog, body, index)
        }

        #[put("/:index")]
        #[content_type("application/json")]
        pub fn add(&self, body: AddDocument, index: String) -> Result<CreatedResponse, Error> {
            if let Ok(ref index_lock) = self.catalog.write() {
                if let Ok(ref index_handle) = index_lock.get_index(&index) {
                    index_handle.add_document(body)?;
                }
            }
            Ok(CreatedResponse)
        }

        #[put("/:index/_create")]
        #[content_type("application/json")]
        pub fn create(&self, body: SchemaBody, index: String) -> impl Future<Item = CreatedResponse, Error = Error> + Send {
            self.inner_create(body, index)
        }
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
        handler.create(body, "new_index".into()).wait().unwrap();
        let search = SearchHandler::new(Arc::clone(&shared_cat));
        let docs = search.get_all_docs("new_index".into()).wait();

        assert_eq!(docs.is_ok(), true);
        assert_eq!(docs.unwrap().hits, 0);
        remove_dir_all::remove_dir_all("new_index")
    }

    #[test]
    fn test_doc_create() {
        let shared_cat = create_test_catalog("test_index");
        let body: AddDocument = serde_json::from_str(
            r#" {"options": {"commit": true}, "document": {"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10} }"#,
        )
        .unwrap();

        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let req = handler.add(body, "test_index".into());

        assert_eq!(req.is_ok(), true);
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
        let req = handler.delete(delete, "test_index".into());

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
        let req = handler.add(add_doc, "test_index".into());
        assert_eq!(req.is_err(), true);
    }
}
