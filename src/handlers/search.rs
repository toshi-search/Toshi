use std::sync::{Arc, RwLock};

use log::info;

use query::Request;
use results::SearchResults;

use crate::handlers::{IndexPath, QueryOptions};
use crate::index::IndexCatalog;

#[derive(Clone)]
pub struct SearchHandler {
    catalog: Arc<RwLock<IndexCatalog>>,
}

impl SearchHandler {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        SearchHandler { catalog }
    }
}

impl_web! {
    impl SearchHandler {

        #[post("/:index")]
        fn doc_search(&self, body: Request, _query_options: QueryOptions, index: IndexPath) -> Result<SearchResults, ()> {
            info!("Query: {:?}", body);
            let docs = self.catalog.read().unwrap().search_index(&index.index, body).map_err(|_| ())?;
            Ok(docs)
        }

        #[get("/:index")]
        fn get_all_docs(&self, query_options: QueryOptions, index: IndexPath) -> Result<SearchResults, ()> {
            self.doc_search(Request::all_docs(), query_options, index)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use hyper::StatusCode;
    use serde_json;

    use index::tests::*;

    use super::*;

    #[derive(Deserialize, Debug)]
    pub struct TestResults {
        pub hits: i32,
        pub docs: Vec<TestSchema>,
    }

    #[derive(Deserialize)]
    pub struct TestDoc {
        pub score: f32,
        pub doc: TestSchema,
    }

    #[derive(Deserialize, Debug)]
    pub struct TestSchema {
        pub score: f32,
        pub test_text: Vec<String>,
        pub test_i64: Vec<i64>,
        pub test_u64: Vec<u64>,
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

        assert_eq!(req.status(), hyper::StatusCode::OK);
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
