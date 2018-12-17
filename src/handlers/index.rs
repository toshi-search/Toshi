use crate::handlers::CreatedResponse;
use crate::index::IndexCatalog;
use crate::Error;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};

use std::path::PathBuf;
use tantivy::directory::MmapDirectory;
use tantivy::schema::*;
use tantivy::Index;

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

    fn add_index(catalog: Arc<RwLock<IndexCatalog>>, name: String, index: Index) -> Result<(), ()> {
        match catalog.write() {
            Ok(ref mut cat) => cat.add_index(name, index).map_err(|_| ()),
            Err(_) => Err(()),
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
    fn delete_document(&self, body: Vec<u8>, index: String) -> Result<DocsAffected, ()> {
        let doc: DeleteDoc = serde_json::from_slice(&body).map_err(|_| ())?;
        if self.catalog.read().unwrap().exists(&index) {
            let docs_affected: u32;
            {
                let index_lock = self.catalog.read().unwrap();
                let index_handle = index_lock.get_index(&index).map_err(|_| ())?;
                let index = index_handle.get_index();
                let index_schema = index.schema();
                let writer_lock = index_handle.get_writer();
                let mut index_writer = writer_lock.lock().map_err(|_| ())?;

                for (field, value) in doc.terms {
                    let mut f = index_schema.get_field(&field).unwrap();
                    let mut term = Term::from_field_text(f, &value);
                    index_writer.delete_term(term);
                }
                if let Some(opts) = doc.options {
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
            Err(())
        }
    }

    #[put("/:index")]
    #[content_type("application/json")]
    fn put_index(&self, body: Vec<u8>, index: String) -> Result<CreatedResponse, ()> {
        let add_doc: AddDocument = serde_json::from_slice(&body).map_err(|_| ())?;
        if let Ok(ref mut index_lock) = self.catalog.write() {
            if let Ok(ref mut index_handle) = &mut index_lock.get_mut_index(&index) {
                {
                    let index = index_handle.get_index();
                    let index_schema = index.schema();
                    let writer_lock = index_handle.get_writer();
                    let mut index_writer = writer_lock.lock().map_err(|_| ())?;
                    let doc: Document = IndexHandler::parse_doc(&index_schema, &add_doc.document.to_string()).map_err(|_| ())?;
                    index_writer.add_document(doc);
                    if let Some(opts) = add_doc.options {
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

    #[put("/:index/create")]
    #[content_type("application/json")]
    fn create(&self, body: Vec<u8>, index: String) -> Result<CreatedResponse, ()> {
        let schema: Schema = serde_json::from_slice(&body).map_err(|_| ())?;
        let ip = self.catalog.read().map_err(|_| ())?.base_path().clone();

        let new_index = IndexHandler::create_from_managed(ip, &index, schema).map_err(|_| ())?;
        IndexHandler::add_index(Arc::clone(&self.catalog), index.clone(), new_index).map(|_| CreatedResponse).map_err(|_| ())
    }
}
}

#[cfg(test)]
mod tests {
    use super::*;
    use index::tests::*;
    use std::fs::remove_file;
    use std::path::PathBuf;

    use hyper::{Method, StatusCode};
    use serde_json;

    #[test]
    fn test_create_index() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let shared_cat = Arc::new(RwLock::new(catalog));

        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_unindex", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;

        {


//            assert_eq!(StatusCode::OK, get_response.status());
//            assert_eq!("{\"hits\":0,\"docs\":[]}", get_response.read_utf8_body().unwrap());

        }
    }

    #[test]
    fn test_doc_create() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = Arc::new(RwLock::new(catalog));

        let body = r#"
        {
          "document": {
            "test_text":    "Babbaboo!",
            "test_u64":     10 ,
            "test_i64":     -10
          }
        }"#;

//        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[test]
    fn test_doc_delete() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = Arc::new(RwLock::new(catalog));

        let body = r#"{
          "options": {"commit": true},
          "terms": {"test_text": "document"}
          }"#;

//        assert_eq!(docs.docs_affected, 3);
    }

    #[test]
    fn test_bad_json() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = Arc::new(RwLock::new(catalog));

        let body = r#"{ "test_text": "document" }"#;


//        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
