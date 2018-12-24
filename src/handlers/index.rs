use crate::handlers::CreatedResponse;
use crate::index::IndexCatalog;
use crate::Error;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};

use serde_derive::{Deserialize, Serialize};
use std::path::PathBuf;
use tantivy::directory::MmapDirectory;
use tantivy::schema::*;
use tantivy::Index;
use tower_web::*;

#[derive(Extract, Deserialize)]
pub struct SchemaBody(Schema);

#[derive(Extract, Deserialize)]
pub struct DeleteDoc {
    options: Option<IndexOptions>,
    terms: HashMap<String, String>,
}

#[derive(Clone)]
pub struct IndexHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

#[derive(Response, Deserialize)]
pub struct DocsAffected {
    docs_affected: u32,
}

#[derive(Extract, Deserialize)]
pub struct IndexOptions {
    #[serde(default)]
    commit: bool,
}

#[derive(Extract, Deserialize)]
pub struct AddDocument {
    options: Option<IndexOptions>,
    document: serde_json::Value,
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

    fn create_from_managed(mut base_path: PathBuf, index_path: &str, schema: Schema) -> Result<Index, Error> {
        base_path.push(index_path);
        if !base_path.exists() {
            fs::create_dir(&base_path).map_err(|e| Error::IOError(e.to_string()))?;
        }
        let dir = MmapDirectory::open(base_path).map_err(|e| Error::IOError(e.to_string()))?;
        Index::open_or_create(dir, schema).map_err(|e| Error::IOError(e.to_string()))
    }

    fn parse_doc(schema: &Schema, bytes: &str) -> Result<Document, Error> {
        schema.parse_document(bytes).map_err(|e| e.into())
    }
}

impl_web! {
    impl IndexHandler {
        #[delete("/:index")]
        #[content_type("application/json")]
        pub fn delete(&self, body: DeleteDoc, index: String) -> Result<DocsAffected, Error> {
            if self.catalog.read().unwrap().exists(&index) {
                let docs_affected: u32;
                {
                    let index_lock = self.catalog.read().unwrap();
                    let index_handle = index_lock.get_index(&index)?;
                    let index = index_handle.get_index();
                    let index_schema = index.schema();
                    let writer_lock = index_handle.get_writer();
                    let mut index_writer = writer_lock.lock()?;

                    for (field, value) in body.terms {
                        let f = index_schema.get_field(&field).unwrap();
                        let term = Term::from_field_text(f, &value);
                        index_writer.delete_term(term);
                    }
                    if let Some(opts) = body.options {
                        if opts.commit {
                            index_writer.commit().unwrap();
                            index_handle.set_opstamp(0);
                        }
                    }
                    docs_affected = index
                        .load_metas()
                        .map(|meta| meta.segments.iter().map(|seg| seg.num_deleted_docs()).sum())
                        .unwrap_or(0);
                }
                Ok(DocsAffected { docs_affected })
            } else {
                Err(Error::IOError("Failed to obtain index lock".into()))
            }
        }

        #[put("/:index")]
        #[content_type("application/json")]
        pub fn add(&self, body: AddDocument, index: String) -> Result<CreatedResponse, Error> {
            if let Ok(ref mut index_lock) = self.catalog.write() {
                if let Ok(ref mut index_handle) = &mut index_lock.get_mut_index(&index) {
                    {
                        let index = index_handle.get_index();
                        let index_schema = index.schema();
                        let writer_lock = index_handle.get_writer();
                        let mut index_writer = writer_lock.lock()?;
                        let doc: Document = IndexHandler::parse_doc(&index_schema, &body.document.to_string())?;
                        index_writer.add_document(doc);
                        if let Some(opts) = body.options {
                            if opts.commit {
                                index_writer.commit().unwrap();
                                index_handle.set_opstamp(0);
                            }
                        } else {
                            index_handle.set_opstamp(index_handle.get_opstamp() + 1);
                        }
                    }
                }
            }
            Ok(CreatedResponse)
        }

        #[put("/:index/_create")]
        #[content_type("application/json")]
        pub fn create(&self, body: SchemaBody, index: String) -> Result<CreatedResponse, Error> {
            let ip = self.catalog.read()?.base_path().clone();
            let new_index = IndexHandler::create_from_managed(ip, &index, body.0)?;
            IndexHandler::add_index(Arc::clone(&self.catalog), index.clone(), new_index).map(|_| CreatedResponse)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::handlers::SearchHandler;
    use crate::index::tests::*;

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
