use super::super::{Error, Result};
use super::*;

use futures::{future, Future, Stream};
use std::fs;
use std::io::Result as IOResult;
use std::panic::RefUnwindSafe;
use std::sync::RwLock;

use tantivy::schema::*;
use tantivy::{Document, Index};

macro_rules! add_field {
    ($METHOD:ident, $S:ident, $D:ident, $F:ident, $A:expr) => {
        $S.get_field(&$F)
            .map(|field| $D.$METHOD(field, $A))
            .ok_or_else(|| Error::UnknownIndexField(format!("Field {} does not exist.", $F)))
    };
}

#[derive(Deserialize, Debug)]
pub struct IndexDoc {
    pub fields: Vec<FieldValues>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum FieldValues {
    StrField { field: String, value: String },
    U64Field { field: String, value: u64 },
    I64Field { field: String, value: i64 },
}

#[derive(Clone, Debug)]
pub struct IndexHandler {
    pub catalog: Arc<RwLock<IndexCatalog>>,
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

    fn add_to_document(schema: &Schema, field: FieldValues, doc: &mut Document) -> Result<()> {
        match field {
            FieldValues::StrField { field, value } => add_field!(add_text, schema, doc, field, &value),
            FieldValues::U64Field { field, value } => add_field!(add_u64, schema, doc, field, value),
            FieldValues::I64Field { field, value } => add_field!(add_i64, schema, doc, field, value),
        }
    }
}

impl Handler for IndexHandler {
    fn handle(mut self, mut state: State) -> Box<HandlerFuture> {
        let url_index = IndexPath::try_take_from(&mut state);
        match url_index {
            Some(ui) => {
                if self.catalog.read().unwrap().exists(&ui.index) {
                    let f = Body::take_from(&mut state).concat2().then(move |body| match body {
                        Ok(b) => {
                            let t: IndexDoc = serde_json::from_slice(&b).unwrap();
                            info!("{:?}", t);
                            {
                                let index_lock = self.catalog.read().unwrap();
                                let index = index_lock.get_index(&ui.index).unwrap();
                                let index_schema = index.schema();
                                let mut index_writer = index.writer(SETTINGS.writer_memory).unwrap();
                                let mut doc = Document::new();
                                for field in t.fields {
                                    match IndexHandler::add_to_document(&index_schema, field, &mut doc) {
                                        Ok(_) => {}
                                        Err(ref e) => return handle_error(state, e),
                                    }
                                }
                                index_writer.add_document(doc);
                                index_writer.commit().unwrap();
                            }
                            let resp = create_response(&state, StatusCode::Created, None);
                            future::ok((state, resp))
                        }
                        Err(ref e) => handle_error(state, e),
                    });
                    Box::new(f)
                } else {
                    let f = Body::take_from(&mut state).concat2().then(move |body| match body {
                        Ok(b) => {
                            let schema: Schema = match serde_json::from_slice(&b) {
                                Ok(v) => v,
                                Err(ref e) => return handle_error(state, e),
                            };
                            let mut index_path = self.catalog.read().unwrap().base_path().clone();
                            index_path.push(&ui.index);
                            if !index_path.exists() {
                                fs::create_dir(&index_path).unwrap()
                            }
                            let new_index = Index::create_in_dir(index_path, schema).unwrap();
                            self.add_index(ui.index, new_index);

                            let resp = create_response(&state, StatusCode::Created, None);
                            future::ok((state, resp))
                        }
                        Err(ref e) => handle_error(state, e),
                    });
                    Box::new(f)
                }
            }
            None => Box::new(handle_error(state, &Error::UnknownIndex("No valid index in path".to_string()))),
        }
    }
}

new_handler!(IndexHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use index::tests::*;

    #[test]
    fn test_serializing() {
        let json = r#"
        {
            "index": "test",
            "fields": [
                {"field": "field1", "value": "sometext"},
                {"field": "field2", "value": 10},
                {"field": "field3", "value": -10}
            ]
        }"#;

        let parsed: IndexDoc = serde_json::from_str(json).unwrap();

        assert_eq!(parsed.fields.len(), 3);
        for f in parsed.fields {
            match f {
                FieldValues::StrField { field, value } => {
                    assert_eq!(field, "field1");
                    assert_eq!(value, "sometext");
                }
                FieldValues::U64Field { field, value } => {
                    assert_eq!(field, "field2");
                    assert_eq!(value, 10u64);
                }
                FieldValues::I64Field { field, value } => {
                    assert_eq!(field, "field3");
                    assert_eq!(value, -10i64);
                }
            }
        }
    }

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
                "fields": [
                    {"field": "test_text", "value": "Babbaboo!" },
                    {"field": "test_u64",  "value": 10 },
                    {"field": "test_i64",  "value": -10 }
                ]
        }"#;

        let response = test_server
            .put("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::Created);
    }
}
