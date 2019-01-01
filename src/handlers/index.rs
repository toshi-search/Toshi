use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tantivy::schema::*;
use tantivy::Index;
use tower_web::*;

use crate::handle::IndexHandle;
use crate::handlers::CreatedResponse;
use crate::index::IndexCatalog;
use crate::Error;

#[derive(Extract, Deserialize)]
pub struct SchemaBody(Schema);

#[derive(Extract, Deserialize)]
pub struct DeleteDoc {
    pub options: Option<IndexOptions>,
    pub terms: HashMap<String, String>,
}

#[derive(Clone)]
pub struct IndexHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

#[derive(Response, Deserialize)]
pub struct DocsAffected {
    pub docs_affected: u32,
}

#[derive(Extract, Deserialize)]
pub struct IndexOptions {
    #[serde(default)]
    pub commit: bool,
}

#[derive(Extract, Deserialize)]
pub struct AddDocument {
    pub options: Option<IndexOptions>,
    pub document: serde_json::Value,
}

impl IndexHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        IndexHandler { catalog }
    }

    fn add_index(catalog: Arc<RwLock<IndexCatalog>>, name: String, index: Index) -> Result<(), Error> {
        match catalog.write() {
            Ok(ref mut cat) => cat.add_index(name, index),
            Err(e) => Err(Error::IOError(e.to_string())),
        }
    }
}

impl_web! {
    impl IndexHandler {
        #[delete("/:index")]
        #[content_type("application/json")]
        pub fn delete(&self, body: DeleteDoc, index: String) -> Result<DocsAffected, Error> {
            if self.catalog.read().unwrap().exists(&index) {
                let index_lock = self.catalog.read().unwrap();
                let index_handle = index_lock.get_index(&index)?;
                index_handle.delete_term(body)
            } else {
                Err(Error::IOError("Failed to obtain index lock".into()))
            }
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
        pub fn create(&self, body: SchemaBody, index: String) -> Result<CreatedResponse, Error> {
            let ip = self.catalog.read()?.base_path().clone();
            let new_index = IndexCatalog::create_from_managed(ip, &index, body.0)?;
            IndexHandler::add_index(Arc::clone(&self.catalog), index.clone(), new_index).map(|_| CreatedResponse)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::handlers::SearchHandler;
    use crate::index::tests::*;

    use super::*;

    #[test]
    fn test_create_index() {
        let shared_cat = create_test_catalog("test_index".into());
        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_unindex", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;
        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let body: SchemaBody = serde_json::from_str(schema).unwrap();
        let req = handler.create(body, "new_index".into());
        assert_eq!(req.is_ok(), true);
        let search = SearchHandler::new(Arc::clone(&shared_cat));
        let docs = search.get_all_docs("new_index".into()).unwrap();
        assert_eq!(docs.hits, 0);
    }

    #[test]
    fn test_doc_create() {
        let shared_cat = create_test_catalog("test_index".into());
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
        let shared_cat = create_test_catalog("test_index".into());
        let handler = IndexHandler::new(Arc::clone(&shared_cat));
        let mut terms = HashMap::new();
        terms.insert("test_text".to_string(), "document".to_string());
        let delete = DeleteDoc {
            options: Some(IndexOptions { commit: true }),
            terms,
        };
        let req = handler.delete(delete, "test_index".into());
        assert_eq!(req.is_ok(), true);
        assert_eq!(req.unwrap().docs_affected, 3);
    }

    #[test]
    fn test_bad_json() {
        let shared_cat = create_test_catalog("test_index".into());
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
