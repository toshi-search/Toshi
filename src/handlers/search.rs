use super::*;

use futures::{future, Future, Stream};
use hyper::Method;

use query::Request;
use std::panic::RefUnwindSafe;
use std::sync::RwLock;

#[derive(Clone)]
pub struct SearchHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl RefUnwindSafe for SearchHandler {}

impl SearchHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self { SearchHandler { catalog } }
}

impl Handler for SearchHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let index = IndexPath::take_from(&mut state);
        let query_options = QueryOptions::take_from(&mut state);
        match *Method::borrow_from(&state) {
            Method::POST => self.doc_search(state, query_options, index),
            Method::GET => self.get_all_docs(state, &query_options, &index),
            _ => unreachable!(),
        }
    }
}

impl SearchHandler {
    fn doc_search(self, mut state: State, query_options: QueryOptions, index: IndexPath) -> Box<HandlerFuture> {
        let f = Body::take_from(&mut state).concat2().then(move |body| match body {
            Ok(b) => {
                let search: Request = match serde_json::from_slice(&b) {
                    Ok(s) => s,
                    Err(e) => return handle_error(state, e),
                };
                info!("Query: {:?}", search);
                let docs = match self.catalog.read().unwrap().search_index(&index.index, search) {
                    Ok(v) => v,
                    Err(e) => return handle_error(state, e),
                };

                let data = to_json(docs, query_options.pretty);
                let resp = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, data);
                future::ok((state, resp))
            }
            Err(e) => handle_error(state, e),
        });
        Box::new(f)
    }

    fn get_all_docs(self, state: State, query_options: &QueryOptions, index: &IndexPath) -> Box<HandlerFuture> {
        if let Ok(idx) = self.catalog.read() {
            match idx.search_index(&index.index, Request::all_docs()) {
                Ok(docs) => {
                    let data = to_json(docs, query_options.pretty);
                    let resp = create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, data);
                    Box::new(future::ok((state, resp)))
                }
                Err(e) => Box::new(handle_error(state, e)),
            }
        } else {
            Box::new(handle_error(state, Error::IOError("Could not obtain lock on index".to_string())))
        }
    }
}

new_handler!(SearchHandler);

#[cfg(test)]
pub mod tests {

    use super::*;
    use index::tests::*;
    use serde_json;

    #[derive(Deserialize, Debug)]
    pub struct TestResults {
        pub hits: i32,
        pub docs: Vec<TestSchema>,
    }

    #[derive(Deserialize)]
    pub struct TestDoc {
        pub score: f32,
        pub doc:   TestSchema,
    }

    #[derive(Deserialize, Debug)]
    pub struct TestSchema {
        pub score:        f32,
        pub test_text:    Vec<String>,
        pub test_i64:     Vec<i64>,
        pub test_u64:     Vec<u64>,
        pub test_unindex: Vec<String>,
    }

    #[derive(Deserialize, Debug)]
    pub struct TestSummaryDoc {
        value: Vec<u64>,
    }

    #[derive(Deserialize, Debug)]
    pub struct TestAgg {
        pub hits: i32,
        pub docs: Vec<TestSummaryDoc>,
    }

    fn run_query(query: &'static str) -> TestResults {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));

        let req = client
            .post("http://localhost/test_index", query, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::OK);
        serde_json::from_slice(&req.read_body().unwrap()).unwrap()
    }

    fn run_agg(query: &'static str) -> TestAgg {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));

        let req = client
            .post("http://localhost/test_index", query, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::OK);
        serde_json::from_slice(&req.read_body().unwrap()).unwrap()
    }

    #[test]
    fn test_term_search() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));

        let req = client.get("http://localhost/test_index").perform().unwrap();
        assert_eq!(req.status(), StatusCode::OK);

        let body = req.read_body().unwrap();
        let docs: TestResults = serde_json::from_slice(&body).unwrap();

        assert_eq!(docs.hits as usize, docs.docs.len());
    }

    #[test]
    fn test_wrong_index_error() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));
        let req = client.get("http://localhost/bad_index").perform().unwrap();

        assert_eq!(req.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_bad_raw_query_syntax() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));
        let body = r#"{ "query" : { "raw": "asd*(@sq__" } }"#;

        let req = client
            .post("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            r#"{"reason":"Query Parse Error: invalid digit found in string"}"#,
            req.read_utf8_body().unwrap()
        )
    }

    #[test]
    fn test_unindexed_field() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));
        let body = r#"{ "query" : { "raw": "test_unindex:asdf" } }"#;

        let req = client
            .post("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_bad_term_field_syntax() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));
        let body = r#"{ "query" : { "term": { "asdf": "Document" } } }"#;

        let req = client
            .post("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            r#"{"reason":"Query Parse Error: Field: asdf does not exist"}"#,
            req.read_utf8_body().unwrap()
        )
    }

    #[test]
    fn test_bad_number_field_syntax() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));
        let body = r#"{ "query" : { "term": { "123asdf": "Document" } } }"#;

        let req = client
            .post("http://localhost/test_index", body, mime::APPLICATION_JSON)
            .perform()
            .unwrap();

        assert_eq!(req.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            r#"{"reason":"Query Parse Error: Field: 123asdf does not exist"}"#,
            req.read_utf8_body().unwrap()
        )
    }

    #[test]
    fn test_bad_method() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let client = create_test_client(&Arc::new(RwLock::new(catalog)));

        let req = client.head("http://localhost/test_index").perform().unwrap();
        assert_eq!(req.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test]
    fn test_raw_query() {
        let body = r#"{ "query" : { "raw": "test_text:5" } }"#;
        let docs = run_query(body);

        assert_eq!(docs.hits as usize, docs.docs.len());
        assert_eq!(docs.docs[0].test_text[0], "Test Document 5")
    }

    #[test]
    fn test_term_query() {
        let body = r#"{ "query" : { "term": { "test_text": "document" } } }"#;
        let docs = run_query(body);

        assert_eq!(docs.hits as usize, docs.docs.len());
        assert_eq!(docs.hits, 3);
        assert_eq!(docs.docs.len(), 3);
    }

    #[test]
    fn test_inclusive_range_query() {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gte" : 2012, "lte" : 2015 } } } }"#;
        let docs = run_query(body);

        assert_eq!(docs.hits as usize, docs.docs.len());
        println!("{:#?}", docs);
        assert_eq!(docs.docs[0].score, 1.0);
    }

    #[test]
    fn test_exclusive_range_query() {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gt" : 2012, "lt" : 2015 } } } }"#;
        let docs = run_query(body);

        assert_eq!(docs.hits as usize, docs.docs.len());
        println!("{:#?}", docs);
        assert_eq!(docs.docs[0].score, 1.0);
    }

    #[test]
    #[ignore]
    fn test_aggregate_sum() {
        let body = r#"{ "query": { "field": "test_u64" } }"#;
        let docs = run_agg(body);

        assert_eq!(docs.docs[0].value[0], 60);
    }
}
