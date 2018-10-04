use super::super::Error;
use super::*;

use futures::{future, Future, Stream};
use std::collections::HashMap;
use std::fs;
use std::io::Result as IOResult;
use std::panic::RefUnwindSafe;
use std::sync::RwLock;

use std::str::from_utf8;

use hyper::Method;
use serde_json::{self, Map as JsonObject, Value as JsonValue};
use tantivy::schema::*;
use tantivy::Index;

#[derive(Deserialize)]
pub struct DeleteDoc {
    terms: HashMap<String, String>,
}

#[derive(Clone)]
pub struct IndexHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

#[derive(Serialize, Deserialize)]
pub struct DocsAffected {
    docs_affected: u32,
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
                    let t: DeleteDoc = match serde_json::from_slice(&b) {
                        Ok(v) => v,
                        Err(e) => return handle_error(state, &Error::IOError(e.to_string())),
                    };
                    let docs_affected: u32;
                    {
                        let index_lock = self.catalog.read().unwrap();
                        let index = index_lock.get_index(&index_path.index).unwrap();
                        let index_schema = index.schema();
                        let mut index_writer = index.writer(SETTINGS.writer_memory).unwrap();

                        for (field, value) in t.terms {
                            let f = match index_schema.get_field(&field) {
                                Some(v) => v,
                                None => return handle_error(state, &Error::UnknownIndexField(field)),
                            };
                            let term = Term::from_field_text(f, &value);
                            index_writer.delete_term(term);
                        }
                        index_writer.commit().unwrap();
                        docs_affected = index.load_metas().unwrap().segments.iter().map(|seg| seg.num_deleted_docs()).sum();
                    }
                    let affected = to_json(DocsAffected { docs_affected }, true);
                    let resp = create_response(&state, StatusCode::Ok, affected);
                    future::ok((state, resp))
                }
                Err(ref e) => handle_error(state, e),
            });
            Box::new(f)
        } else {
            Box::new(handle_error(state, &Error::UnknownIndex(index_path.index)))
        }
    }

    fn add_document(self, mut state: State, index_path: IndexPath) -> Box<HandlerFuture> {
        Box::new(Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(b) => {
                {
                    let index_lock = self.catalog.read().unwrap();
                    let index = index_lock.get_index(&index_path.index).unwrap();
                    let index_schema = index.schema();
                    let mut index_writer = index.writer(SETTINGS.writer_memory).unwrap();
                    let mut doc = index_schema.parse_document(from_utf8(&b).unwrap()).unwrap();
                    index_writer.add_document(doc);

                    //index_writer.commit().unwrap();
                }
                let resp = create_response(&state, StatusCode::Created, None);
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
                let new_index = Index::create_in_dir(ip, schema).unwrap();
                self.add_index(index_path.index, new_index);
                let resp = create_response(&state, StatusCode::Created, None);
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
                Method::Delete => self.delete_document(state, ui),
                Method::Put => {
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
    use hyper::header::ContentType;
    use index::tests::*;

    #[test]
    fn test_create_index() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let shared_cat = Arc::new(RwLock::new(catalog));
        let test_server = create_test_server(&shared_cat);

        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;

        let request = test_server
            .client()
            .put("http://localhost/new_index", schema, mime::APPLICATION_JSON);
        let response = &request.perform().unwrap();

        assert_eq!(response.status(), StatusCode::Created);

        let get_request = test_server.client().get("http://localhost/new_index");
        let get_response = get_request.perform().unwrap();

        assert_eq!(StatusCode::Ok, get_response.status());
        assert_eq!("{\"hits\":0,\"docs\":[]}", get_response.read_utf8_body().unwrap())
    }

    #[test]
    fn test_doc_create() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = create_test_client(&Arc::new(RwLock::new(catalog)));

        let body = r#"
        {
            "test_text":    "Babbaboo!",
            "test_u64":     10 ,
            "test_i64":     -10
        }"#;

        let response = test_server
            .put("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::Created);
    }

    #[test]
    fn test_doc_delete() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let test_server = create_test_client(&Arc::new(RwLock::new(catalog)));

        let body = r#"{ "terms": {"test_text": "document"} }"#;

        let response = test_server
            .delete("http://localhost/test_index")
            .with_body(body)
            .with_header(ContentType(mime::APPLICATION_JSON))
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::Ok);

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
            .delete("http://localhost/test_index")
            .with_body(body)
            .with_header(ContentType(mime::APPLICATION_JSON))
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::BadRequest);
    }
}
