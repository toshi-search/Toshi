use crate::handlers::{CreatedResponse, IndexPath};
use crate::index::IndexCatalog;
use crate::Error;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};

use tantivy::directory::MmapDirectory;
use tantivy::schema::*;
use tantivy::Index;
use std::path::PathBuf;

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

    fn add_index(&mut self, name: String, index: Index) -> Result<(), ()> {
        match self.catalog.write() {
            Ok(ref mut cat) => Ok(cat.add_index(name, index)),
            Err(_) => Err(()),
        }
    }

    fn create_from_managed(mut base_path: PathBuf, index_path: &IndexPath, schema: Schema) -> Result<Index, Error> {
        base_path.push(&index_path.index);
        if !base_path.exists() {
            fs::create_dir(&base_path).map_err(|e| Error::IOError(e.to_string()))?;
        }
        let dir = MmapDirectory::open(base_path).map_err(|e| Error::IOError(e.to_string()))?;
        Index::open_or_create(dir, schema).map_err(|e| Error::IOError(e.to_string()))
    }

    fn parse_doc(schema: &Schema, bytes: &str) -> Result<Document, Error> {
        schema.parse_document(bytes).map_err(|e| e.into())
    }

    fn create_index(&mut self, base_path: PathBuf, body: Schema, index_path: IndexPath) -> Result<CreatedResponse, ()> {
        let new_index = IndexHandler::create_from_managed(base_path, &index_path, body).map_err(|_| ())?;
        self.add_index(index_path.index, new_index);
        Ok(CreatedResponse)
    }

    fn add_document(&mut self, body: AddDocument, index_path: IndexPath) -> Result<CreatedResponse, ()> {
        if let Ok(ref mut index_lock) = self.catalog.write() {
            if let Ok(ref mut index_handle) = &mut index_lock.get_mut_index(&index_path.index) {
                {
                    let index = index_handle.get_index();
                    let index_schema = index.schema();
                    let writer_lock = index_handle.get_writer();
                    let mut index_writer = writer_lock.lock().map_err(|_| ())?;
                    let doc: Document = IndexHandler::parse_doc(&index_schema, &body.document.to_string()).map_err(|_| ())?;
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
}
impl_web! {
    impl IndexHandler {

        #[post("/:index/_delete")]
        fn delete_document(&mut self, body: Vec<u8>, index: IndexPath) -> Result<DocsAffected, ()> {
            let doc: DeleteDoc = serde_json::from_slice(&body).map_err(|_| ())?;
            if self.catalog.read().unwrap().exists(&index.index) {
                let docs_affected: u32;
                {
                    let index_lock = self.catalog.read().unwrap();
                    let index_handle = index_lock.get_index(&index.index).map_err(|_| ())?;
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

        #[post("/:index")]
        fn put_index(&mut self, body: Vec<u8>, index: IndexPath) -> Result<CreatedResponse, ()> {
            let doc: AddDocument = serde_json::from_slice(&body).map_err(|_| ())?;
            self.add_document(doc, index)
        }


        #[post("/:index/_create")]
        fn create(&mut self, body: Vec<u8>, index: IndexPath) -> Result<CreatedResponse, ()> {
            let schema: Schema = serde_json::from_slice(&body).map_err(|_| ())?;
            let ip = self.catalog.read().map_err(|_| ())?.base_path().clone();

            self.create_index(ip, schema, index)
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
        let test_server = create_test_server(&shared_cat);

        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_unindex", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;

        {
            let client = test_server.client();
            let request = client.put("http://localhost/new_index", schema, mime::APPLICATION_JSON);
            let response = request.perform().unwrap();

            assert_eq!(response.status(), StatusCode::CREATED);

            let get_request = client.get("http://localhost/new_index");
            let get_response = get_request.perform().unwrap();

            assert_eq!(StatusCode::OK, get_response.status());
            assert_eq!("{\"hits\":0,\"docs\":[]}", get_response.read_utf8_body().unwrap());
            let mut p = PathBuf::from("new_index");
            p.push(".tantivy-indexer.lock");
            remove_file(p).unwrap();
        }
    }

    #[test]
    fn test_doc_create() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = create_test_client(&Arc::new(RwLock::new(catalog)));

        let body = r#"
        {
          "document": {
            "test_text":    "Babbaboo!",
            "test_u64":     10 ,
            "test_i64":     -10
          }
        }"#;

        let response = test_server
            .put("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[test]
    fn test_doc_delete() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = create_test_client(&Arc::new(RwLock::new(catalog)));

        let body = r#"{
          "options": {"commit": true},
          "terms": {"test_text": "document"}
          }"#;

        let response = test_server
            .build_request_with_body(Method::DELETE, "http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let docs: DocsAffected = serde_json::from_slice(&response.read_body().unwrap()).unwrap();
        assert_eq!(docs.docs_affected, 3);
    }

    #[test]
    fn test_bad_json() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = create_test_client(&Arc::new(RwLock::new(catalog)));

        let body = r#"{ "test_text": "document" }"#;

        let response = test_server
            .build_request_with_body(Method::DELETE, "http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
