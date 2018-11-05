use super::*;

use futures::{future, Future, Stream};
use std::collections::HashMap;
use std::fs;
use std::panic::RefUnwindSafe;
use std::sync::RwLock;

use hyper::{Method, StatusCode};
use tantivy::schema::*;
use tantivy::Index;

#[derive(Deserialize)]
pub struct DeleteDoc {
    options: Option<IndexOptions>,
    terms:   HashMap<String, String>,
}

#[derive(Clone)]
pub struct IndexHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

#[derive(Serialize, Deserialize)]
pub struct DocsAffected {
    docs_affected: u32,
}

#[derive(Deserialize)]
pub struct IndexOptions {
    #[serde(default)]
    commit: bool,
}

#[derive(Deserialize)]
pub struct AddDocument {
    options:  Option<IndexOptions>,
    document: serde_json::Value,
}

impl RefUnwindSafe for IndexHandler {}

impl IndexHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self { IndexHandler { catalog } }

    fn add_index(&mut self, name: String, index: Index) {
        match self.catalog.write() {
            Ok(ref mut cat) => cat.add_index(name, index),
            Err(e) => panic!("{}", e),
        }
    }

    fn delete_document(self, mut state: State, index_path: IndexPath) -> Box<HandlerFuture> {
        if self.catalog.read().unwrap().exists(&index_path.index) {
            let f = Body::take_from(&mut state).concat2().then(move |body| match body {
                Ok(b) => {
                    let delete_doc: DeleteDoc = match serde_json::from_slice(&b) {
                        Ok(v) => v,
                        Err(e) => return handle_error(state, &Error::IOError(e.to_string())),
                    };
                    let docs_affected: u32;
                    {
                        let index_lock = self.catalog.read().unwrap();
                        let index_handle = index_lock.get_index(&index_path.index).unwrap();
                        let index = index_handle.get_index();
                        let index_schema = index.schema();
                        let writer_lock = index_handle.get_writer();
                        let mut index_writer = match writer_lock.lock() {
                            Ok(w) => w,
                            Err(ref e) => return handle_error(state, &Error::IOError(e.to_string())),
                        };

                        for (field, value) in delete_doc.terms {
                            let f = match index_schema.get_field(&field) {
                                Some(v) => v,
                                None => return handle_error(state, &Error::UnknownIndexField(field)),
                            };
                            let term = Term::from_field_text(f, &value);
                            index_writer.delete_term(term);
                        }
                        if let Some(opts) = delete_doc.options {
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
                    let body = to_json(DocsAffected { docs_affected }, true);
                    let resp = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, body);
                    future::ok((state, resp))
                }
                Err(ref e) => handle_error(state, e),
            });
            Box::new(f)
        } else {
            Box::new(handle_error(state, &Error::UnknownIndex(index_path.index)))
        }
    }

    fn parse_doc(&self, schema: &Schema, bytes: &str) -> Result<Document> { schema.parse_document(bytes).map_err(|e| e.into()) }

    fn add_document(self, mut state: State, index_path: IndexPath) -> Box<HandlerFuture> {
        Box::new(Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(ref b) => {
                if let Ok(mut index_lock) = self.catalog.write() {
                    if let Ok(index_handle) = index_lock.get_mut_index(&index_path.index) {
                        {
                            let add_document: AddDocument = serde_json::from_slice(b).unwrap();
                            let index = index_handle.get_index();
                            let index_schema = index.schema();
                            let writer_lock = index_handle.get_writer();
                            let mut index_writer = match writer_lock.lock() {
                                Ok(w) => w,
                                Err(ref e) => return handle_error(state, &Error::IOError(e.to_string())),
                            };
                            let mut doc = match self.parse_doc(&index_schema, &add_document.document.to_string()) {
                                Ok(d) => d,
                                Err(ref e) => return handle_error(state, e),
                            };
                            index_writer.add_document(doc);
                            if let Some(opts) = add_document.options {
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
                let resp = create_empty_response(&state, StatusCode::CREATED);
                future::ok((state, resp))
            }
            Err(ref e) => handle_error(state, e),
        }))
    }

    fn create_index(mut self, mut state: State, index_path: IndexPath) -> Box<HandlerFuture> {
        Box::new(Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(b) => {
                let schema: Schema = match serde_json::from_slice(&b) {
                    Ok(v) => v,
                    Err(ref e) => return handle_error(state, e),
                };
                let mut ip = self.catalog.read().unwrap().base_path().clone();
                ip.push(&index_path.index);
                if !ip.exists() {
                    fs::create_dir(&ip).unwrap()
                }

                let new_index = match Index::create_in_dir(ip, schema) {
                    Ok(i) => i,
                    Err(ref e) => return handle_error(state, e),
                };
                self.add_index(index_path.index, new_index);
                let resp = create_response(&state, StatusCode::CREATED, mime::APPLICATION_JSON, Body::empty());
                future::ok((state, resp))
            }
            Err(ref e) => handle_error(state, e),
        }))
    }
}

impl Handler for IndexHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let url_index = IndexPath::try_take_from(&mut state);
        match url_index {
            Some(ui) => match *Method::borrow_from(&state) {
                Method::DELETE => self.delete_document(state, ui),
                Method::PUT => {
                    if self.catalog.read().unwrap().exists(&ui.index) {
                        self.add_document(state, ui)
                    } else {
                        self.create_index(state, ui)
                    }
                }
                _ => unreachable!(),
            },
            None => Box::new(handle_error(state, &Error::UnknownIndex("No valid index in path".to_string()))),
        }
    }
}

new_handler!(IndexHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use index::tests::*;
    use std::fs::remove_file;
    use std::path::PathBuf;

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
