use std::io::Result as IOResult;
use std::panic::RefUnwindSafe;

use futures::{future, Future, Stream};

use tantivy::schema::*;
use tantivy::Document;

use super::super::{Error, Result};
use super::*;

macro_rules! add_field {
    ($METHOD:ident, $S:ident, $D:ident, $F:ident, $A:expr) => {
        $S.get_field(&$F)
            .map(|field| $D.$METHOD(field, $A))
            .ok_or_else(|| Error::UnknownIndexField(format!("Field {} does not exist.", $F)))
    };
}

#[derive(Deserialize, Debug)]
pub struct IndexDoc {
    pub index:  String,
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
    catalog: Arc<IndexCatalog>,
}

impl RefUnwindSafe for IndexHandler {}

impl IndexHandler {
    pub fn new(catalog: Arc<IndexCatalog>) -> Self { IndexHandler { catalog } }

    fn add_to_document(schema: &Schema, field: FieldValues, doc: &mut Document) -> Result<()> {
        match field {
            FieldValues::StrField { field, value } => add_field!(add_text, schema, doc, field, &value),
            FieldValues::U64Field { field, value } => add_field!(add_u64, schema, doc, field, value),
            FieldValues::I64Field { field, value } => add_field!(add_i64, schema, doc, field, value),
        }
    }
}

impl Handler for IndexHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let f = Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(b) => {
                let t: IndexDoc = serde_json::from_slice(&b).unwrap();
                info!("{:?}", t);
                {
                    let index = match self.catalog.get_index(&t.index) {
                        Ok(i) => i,
                        Err(ref e) => return handle_error(state, e),
                    };
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
    }
}

new_handler!(IndexHandler);

#[cfg(test)]
mod tests {

    use super::*;
    use index::tests::*;
    use gotham::test::*;


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
        assert_eq!(&parsed.index, "test");
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
    fn test_indexes() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let handler = IndexHandler::new(Arc::new(catalog));
        let test_server = TestServer::new(handler).unwrap();
        let body = r#"
        {
            "index": "test_index",
                "fields": [
                    {"field": "test_text", "value": "Babbaboo!" },
                    {"field": "test_u64",  "value": 10 },
                    {"field": "test_i64",  "value": -10 }
                ]
        }"#;

        let response = test_server
            .client()
            .put("http://localhost/", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::Created);
    }
}
