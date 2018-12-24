use std::sync::{Arc, RwLock};

use log::info;
use tower_web::*;

use crate::index::IndexCatalog;
use crate::query::Request;
use crate::results::SearchResults;
use crate::Error;

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
        #[content_type("application/json")]
        pub fn doc_search(&self, body: Request, index: String) -> Result<SearchResults, Error> {
            info!("Query: {:?}", body);
            self.catalog.read().unwrap()
                .search_index(&index, body)
        }

        #[get("/:index")]
        #[content_type("application/json")]
        pub fn get_all_docs(&self, index: String) -> Result<SearchResults, Error> {
            self.catalog.read().unwrap()
                .search_index(&index, Request::all_docs())
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::index::tests::*;
    use crate::query::*;
    use std::collections::HashMap;

    pub fn make_map<V>(field: &'static str, term: V) -> HashMap<String, V> {
        let mut term_map = HashMap::<String, V>::new();
        term_map.insert(field.into(), term);
        term_map
    }

    pub fn run_query(req: Request, index: &str) -> Result<SearchResults, Error> {
        let cat = create_test_catalog(index.into());
        let handler = SearchHandler::new(Arc::clone(&cat));
        handler.doc_search(req, index.into())
    }

    #[test]
    fn test_term_query() {
        let term = make_map("test_text", String::from("document"));
        let term_query = Query::Exact(ExactTerm { term });
        let search = Request::new(Some(term_query), None, 10);
        let query = run_query(search, "test_index");
        assert_eq!(query.is_ok(), true);
        let results = query.unwrap();
        assert_eq!(results.hits, 3);
    }

    #[test]
    fn test_wrong_index_error() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "test_text:\"document\"" } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        let docs = handler.doc_search(req, "asdf".into());
        match docs {
            Err(Error::UnknownIndex(e)) => assert_eq!(e.to_string(), "asdf"),
            _ => assert_eq!(true, false),
        }
    }

    #[test]
    fn test_bad_raw_query_syntax() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "asd*(@sq__" } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        let docs = handler.doc_search(req, "test_index".into());
        match docs {
            Err(Error::QueryError(e)) => assert_eq!(e.to_string(), "invalid digit found in string"),
            _ => assert_eq!(true, false),
        }
    }

    #[test]
    fn test_unindexed_field() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "test_unindex:asdf" } }"#;

        let req: Request = serde_json::from_str(body).unwrap();
        let docs = handler.doc_search(req, "test_index".into());
        match docs {
            Err(Error::QueryError(e)) => assert_eq!(e.to_string(), "Query to unindexed field \'test_unindex\'"),
            _ => assert_eq!(true, false),
        }
    }

    #[test]
    fn test_bad_term_field_syntax() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "term": { "asdf": "Document" } } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        let docs = handler.doc_search(req, "test_index".into());
        match docs {
            Err(Error::QueryError(e)) => assert_eq!(e.to_string(), "Field: asdf does not exist"),
            _ => assert_eq!(true, false),
        }
    }

    #[test]
    fn test_raw_query() {
        let body = r#"test_text:"Duckiment""#;
        let req = Request::new(Some(Query::Raw { raw: body.into() }), None, 10);
        let docs = run_query(req, "test_index");
        assert_eq!(docs.is_ok(), true);
        let result = docs.unwrap();

        assert_eq!(result.hits as usize, result.docs.len());
        assert_eq!(
            result.docs[0].doc.0.get("test_text").unwrap()[0].text().unwrap(),
            "Test Duckiment 3"
        )
    }

    #[test]
    fn test_fuzzy_term_query() {
        let fuzzy = make_map("test_text", FuzzyTerm::new("document".into(), 0, false));
        let term_query = Query::Fuzzy(FuzzyQuery { fuzzy });
        let search = Request::new(Some(term_query), None, 10);
        let query = run_query(search, "test_index");
        assert_eq!(query.is_ok(), true);
        let result = query.unwrap();

        assert_eq!(result.hits as usize, result.docs.len());
        assert_eq!(result.hits, 3);
        assert_eq!(result.docs.len(), 3);
    }

    #[test]
    fn test_inclusive_range_query() {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gte" : 2012, "lte" : 2015 } } } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        let docs = run_query(req, "test_index");
        assert_eq!(docs.is_ok(), true);
        let result = docs.unwrap();
        assert_eq!(result.hits as usize, result.docs.len());
        assert_eq!(result.docs[0].score.unwrap(), 1.0);
    }

    #[test]
    fn test_exclusive_range_query() {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gt" : 2012, "lt" : 2015 } } } }"#;
        let req: Request = serde_json::from_str(&body).unwrap();
        let docs = run_query(req, "test_index");
        assert_eq!(docs.is_ok(), true);
        let results = docs.unwrap();
        assert_eq!(results.hits as usize, results.docs.len());
        assert_eq!(results.docs[0].score.unwrap(), 1.0);
    }

    // This is ignored right now while we wait for https://github.com/tantivy-search/tantivy/pull/437
    // to be released.
    //#[test]
    //#[ignore]
    //fn test_aggregate_sum() {
    //    let _body = r#"{ "query": { "field": "test_u64" } }"#;
    //}
}
