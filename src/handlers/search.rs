use std::sync::{Arc, RwLock};

use futures::prelude::*;
use futures::stream::futures_unordered;
use log::info;
use tokio::prelude::*;
use tower_web::*;

use crate::index::IndexCatalog;
use crate::query::Request;
use crate::results::ScoredDoc;
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

    fn fold_results(results: Vec<SearchResults>) -> SearchResults {
        let mut docs: Vec<ScoredDoc> = Vec::new();
        for result in results {
            docs.extend(result.docs);
        }
        SearchResults::new(docs)
    }

    fn inner_doc_search(&self, body: Request, index: String) -> impl Future<Item = SearchResults, Error = Error> + Send {
        info!("Query: {:?}", body);
        match self.catalog.read() {
            Ok(c) => {
                let tasks = vec![
                    future::Either::A(c.search_local_index(&index, body.clone())),
                    future::Either::B(c.search_remote_index(&index, body.clone())),
                ];
                futures_unordered(tasks)
                    .then(|next| match next {
                        Ok(v) => Ok(v),
                        Err(_) => Ok(Vec::new()),
                    })
                    .concat2()
                    .map(SearchHandler::fold_results)
            }
            Err(_) => panic!(),
        }
    }
}

impl_web! {
    impl SearchHandler {
        #[post("/:index")]
        #[content_type("application/json")]
        pub fn doc_search(&self, body: Request, index: String) -> impl Future<Item = SearchResults, Error = Error> + Send {
            self.inner_doc_search(body, index)
        }

        #[get("/:index")]
        #[content_type("application/json")]
        pub fn get_all_docs(&self, index: String) -> impl Future<Item = SearchResults, Error = Error> + Send {
            self.doc_search(Request::all_docs(), index)
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::index::tests::*;
    use crate::query::*;

    pub fn run_query(req: Request, index: &str) -> impl Future<Item = SearchResults, Error = Error> + Send {
        let cat = create_test_catalog(index.into());
        let handler = SearchHandler::new(Arc::clone(&cat));
        handler.doc_search(req, index.into())
    }

    #[test]
    fn test_term_query() {
        let term = KeyValue::new("test_text".into(), "document".into());
        let term_query = Query::Exact(ExactTerm::new(term));
        let search = Request::new(Some(term_query), None, 10);
        run_query(search, "test_index")
            .map(|q| {
                dbg!(&q);
                assert_eq!(q.hits, 3);
            })
            .wait()
            .unwrap();
    }

    #[test]
    fn test_phrase_query() {
        let terms = TermPair::new(vec!["test".into(), "document".into()], None);
        let phrase = KeyValue::new("test_text".into(), terms);
        let term_query = Query::Phrase(PhraseQuery::new(phrase));
        let search = Request::new(Some(term_query), None, 10);
        run_query(search, "test_index")
            .map(|q| {
                dbg!(&q);
                assert_eq!(q.hits, 3);
            })
            .wait()
            .unwrap();
    }

    #[test]
    #[allow(unused_must_use)]
    fn test_wrong_index_error() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "test_text:\"document\"" } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        handler
            .doc_search(req, "asdf".into())
            .map_err(|err| assert_eq!(err.to_string(), "Unknown Index: \'asdf\' does not exist"))
            .map(|d| dbg!(d))
            .wait();
    }

    #[test]
    #[allow(unused)]
    fn test_bad_raw_query_syntax() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "asd*(@sq__" } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        handler
            .doc_search(req, "test_index".into())
            .map_err(|err| {
                dbg!(&err.to_string());
                assert_eq!(err.to_string(), "Query Parse Error: invalid digit found in string");
            })
            .wait();
    }

    #[test]
    fn test_unindexed_field() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "test_unindex:asdf" } }"#;

        let req: Request = serde_json::from_str(body).unwrap();
        let docs = handler
            .doc_search(req, "test_index".into())
            .map_err(|err| match err {
                Error::QueryError(e) => assert_eq!(e.to_string(), "Query to unindexed field \'test_unindex\'"),
                _ => assert_eq!(true, false),
            })
            .map(|_| ());

        tokio::run(docs);
    }

    #[test]
    fn test_bad_term_field_syntax() {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "term": { "asdf": "Document" } } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        let docs = handler
            .doc_search(req, "test_index".into())
            .map_err(|err| match err {
                Error::QueryError(e) => assert_eq!(e.to_string(), "Field: asdf does not exist"),
                _ => assert_eq!(true, false),
            })
            .map(|_| ());

        tokio::run(docs);
    }

    #[test]
    fn test_raw_query() {
        let body = r#"test_text:"Duckiment""#;
        let req = Request::new(Some(Query::Raw { raw: body.into() }), None, 10);
        let docs = run_query(req, "test_index")
            .map(|result| {
                assert_eq!(result.hits as usize, result.docs.len());
                assert_eq!(result.docs[0].doc.get("test_text").unwrap()[0].text().unwrap(), "Test Duckiment 3")
            })
            .map_err(|_| ());

        tokio::run(docs);
    }

    #[test]
    fn test_fuzzy_term_query() {
        let fuzzy = KeyValue::new("test_text".into(), FuzzyTerm::new("document".into(), 0, false));
        let term_query = Query::Fuzzy(FuzzyQuery::new(fuzzy));
        let search = Request::new(Some(term_query), None, 10);
        let query = run_query(search, "test_index")
            .map(|result| {
                assert_eq!(result.hits as usize, result.docs.len());
                assert_eq!(result.hits, 3);
                assert_eq!(result.docs.len(), 3);
            })
            .map_err(|_| ());

        tokio::run(query);
    }

    #[test]
    fn test_inclusive_range_query() {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gte" : 2012, "lte" : 2015 } } } }"#;
        let req: Request = serde_json::from_str(body).unwrap();
        let docs = run_query(req, "test_index")
            .map(|result| {
                assert_eq!(result.hits as usize, result.docs.len());
                assert_eq!(result.docs[0].score.unwrap(), 1.0);
            })
            .map_err(|_| ());

        tokio::run(docs);
    }

    #[test]
    fn test_exclusive_range_query() {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gt" : 2012, "lt" : 2015 } } } }"#;
        let req: Request = serde_json::from_str(&body).unwrap();
        let docs = run_query(req, "test_index")
            .map(|results| {
                assert_eq!(results.hits as usize, results.docs.len());
                assert_eq!(results.docs[0].score.unwrap(), 1.0);
            })
            .map_err(|_| ());

        tokio::run(docs);
    }

    #[test]
    fn test_regex_query() {
        let body = r#"{ "query" : { "regex" : { "test_text" : "d[ou]{1}c[k]?ument" } } }"#;
        let req: Request = serde_json::from_str(&body).unwrap();
        let docs = run_query(req, "test_index")
            .map(|results| assert_eq!(results.hits, 4))
            .map_err(|_| ());

        tokio::run(docs);
    }

    // This is ignored right now while we wait for https://github.com/tantivy-search/tantivy/pull/437
    // to be released.
    //#[test]
    //#[ignore]
    //fn test_aggregate_sum() {
    //    let _body = r#"{ "query": { "field": "test_u64" } }"#;
    //}
}
