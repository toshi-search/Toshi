use std::sync::Arc;

use futures::future::Either;
use futures::stream::futures_unordered;
use http::StatusCode;
use hyper::Body;
use tokio::prelude::*;
use tracing::*;

use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::utils::{empty_with_code, with_body};
use crate::SearchResults;
use toshi_types::query::Search;

#[derive(Clone)]
pub struct SearchHandler {
    catalog: SharedCatalog,
}

impl SearchHandler {
    pub fn new(catalog: SharedCatalog) -> Self {
        SearchHandler { catalog }
    }

    #[inline]
    fn fold_results(results: Vec<SearchResults>) -> SearchResults {
        results.into_iter().sum()
    }

    pub fn doc_search(&self, body: Body, index: String) -> ResponseFuture {
        let catalog = Arc::clone(&self.catalog);
        Box::new(
            body.concat2()
                .map(|b| serde_json::from_slice::<Search>(&b).unwrap())
                .and_then(move |req| {
                    let c = catalog.read();
                    let req = if req.query.is_none() { Search::all_docs() } else { req };
                    info!("Query: {:?}", req);
                    if c.exists(&index) {
                        let mut tasks = vec![future::Either::A(c.search_local_index(&index, req.clone()))];
                        if c.remote_exists(&index) {
                            tasks.push(future::Either::B(c.search_remote_index(&index, req)));
                        }
                        Either::A(
                            futures_unordered(tasks)
                                .then(|next| match next {
                                    Ok(v) => Ok(v),
                                    Err(_) => Ok(Vec::new()),
                                })
                                .concat2()
                                .map(SearchHandler::fold_results)
                                .map(with_body),
                        )
                    } else {
                        Either::B(future::ok(empty_with_code(StatusCode::NOT_FOUND)))
                    }
                }),
        )
    }

    pub fn all_docs(&self, index: String) -> ResponseFuture {
        let body = Body::from(serde_json::to_vec(&Search::all_docs()).unwrap());
        self.doc_search(body, index)
    }
}

#[cfg(test)]
pub mod tests {
    use hyper::Response;
    use pretty_assertions::assert_eq;
    use serde::de::DeserializeOwned;

    use crate::handlers::ResponseFuture;
    use crate::index::tests::*;
    use toshi_types::query::*;

    use super::*;
    use toshi_types::query::{KeyValue, Query};

    type ReturnUnit = Result<(), hyper::error::Error>;

    pub fn wait_json<T: DeserializeOwned>(r: Response<Body>) -> T {
        r.into_body()
            .concat2()
            .map(|ref b| serde_json::from_slice::<T>(b).unwrap_or_else(|_| panic!("Could not deserialize JSON: {:?}", b)))
            .wait()
            .unwrap_or_else(|e| panic!(e))
    }

    pub fn run_query(req: Search, index: &str) -> ResponseFuture {
        let cat = create_test_catalog(index);
        let handler = SearchHandler::new(Arc::clone(&cat));
        handler.doc_search(Body::from(serde_json::to_vec(&req).unwrap()), index.into())
    }

    #[test]
    fn test_term_query() {
        let term = KeyValue::new("test_text".into(), "document".into());
        let term_query = Query::Exact(ExactTerm::new(term));
        let search = Search::new(Some(term_query), None, 10);
        run_query(search, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);
                assert_eq!(body.hits, 3);
            })
            .wait()
            .unwrap();
    }

    #[test]
    fn test_phrase_query() {
        let terms = TermPair::new(vec!["test".into(), "document".into()], None);
        let phrase = KeyValue::new("test_text".into(), terms);
        let term_query = Query::Phrase(PhraseQuery::new(phrase));
        let search = Search::new(Some(term_query), None, 10);
        run_query(search, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);
                assert_eq!(body.hits, 3);
            })
            .wait()
            .unwrap();
    }

    #[test]
    fn test_wrong_index_error() -> ReturnUnit {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "test_text:\"document\"" } }"#;

        handler
            .doc_search(Body::from(body), "asdf".into())
            .map(|_| ())
            .map_err(|err| {
                assert_eq!(err.to_string(), "Unknown Index: \'asdf\' does not exist");
                err
            })
            .wait()
    }

    #[test]
    fn test_bad_raw_query_syntax() -> ReturnUnit {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "asd*(@sq__" } }"#;

        handler
            .doc_search(Body::from(body), "test_index".into())
            .map(|_| ())
            .map_err(|err| {
                assert_eq!(err.to_string(), "Query Parse Error: invalid digit found in string");
                err
            })
            .wait()
    }

    #[test]
    fn test_unindexed_field() -> ReturnUnit {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "raw": "test_unindex:yes" } }"#;

        handler
            .doc_search(Body::from(body), "test_index".into())
            .map(|r| {
                let docs: SearchResults = wait_json(r);
                assert_eq!(docs.hits, 0);
            })
            .map_err(|err| dbg!(err))
            .wait()
    }

    #[test]
    fn test_bad_term_field_syntax() -> Result<(), serde_json::Error> {
        let cat = create_test_catalog("test_index");
        let handler = SearchHandler::new(Arc::clone(&cat));
        let body = r#"{ "query" : { "term": { "asdf": "Document" } } }"#;
        let _req: Search = serde_json::from_str(body)?;
        let docs = handler
            .doc_search(Body::from(body), "test_index".into())
            .map(|_| ())
            .map_err(|_| ());

        tokio::run(docs);
        Ok(())
    }

    #[test]
    fn test_facets() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "term": { "test_text": "document" } }, "facets": { "test_facet": ["/cat"] } }"#;
        let req: Search = serde_json::from_str(body)?;
        let docs = run_query(req, "test_index")
            .map(|q| {
                let b: SearchResults = wait_json(q);
                assert_eq!(b.facets[0].value, 1);
                assert_eq!(b.facets[1].value, 1);
                assert_eq!(b.facets[0].field, "/cat/cat2");
            })
            .map_err(|_| ());

        tokio::run(docs);
        Ok(())
    }

    #[test]
    fn test_raw_query() -> Result<(), serde_json::Error> {
        let body = r#"test_text:"Duckiment""#;
        let req = Search::new(Some(Query::Raw { raw: body.into() }), None, 10);
        let docs = run_query(req, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);
                assert_eq!(body.hits as usize, body.docs.len());
                assert_eq!(body.docs[0].doc["test_text"][0].text().unwrap(), "Test Duckiment 3")
            })
            .map_err(|_| ());

        tokio::run(docs);
        Ok(())
    }

    #[test]
    fn test_fuzzy_term_query() -> Result<(), serde_json::Error> {
        let fuzzy = KeyValue::new("test_text".into(), FuzzyTerm::new("document".into(), 0, false));
        let term_query = Query::Fuzzy(FuzzyQuery::new(fuzzy));
        let search = Search::new(Some(term_query), None, 10);
        let query = run_query(search, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);

                assert_eq!(body.hits as usize, body.docs.len());
                assert_eq!(body.hits, 3);
                assert_eq!(body.docs.len(), 3);
            })
            .map_err(|_| ());

        tokio::run(query);
        Ok(())
    }

    #[test]
    fn test_inclusive_range_query() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gte" : 2012, "lte" : 2015 } } } }"#;
        let req: Search = serde_json::from_str(body)?;
        let docs = run_query(req, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);

                assert_eq!(body.hits as usize, body.docs.len());
                assert_eq!(body.docs[0].score.unwrap(), 1.0);
            })
            .map_err(|_| ());

        tokio::run(docs);
        Ok(())
    }

    #[test]
    fn test_exclusive_range_query() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gt" : 2012, "lt" : 2015 } } } }"#;
        let req: Search = serde_json::from_str(&body)?;
        let docs = run_query(req, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);

                assert_eq!(body.hits as usize, body.docs.len());
                assert_eq!(body.docs[0].score.unwrap(), 1.0);
            })
            .map_err(|_| ());

        tokio::run(docs);
        Ok(())
    }

    #[test]
    fn test_regex_query() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "regex" : { "test_text" : "d[ou]{1}c[k]?ument" } } }"#;
        let req: Search = serde_json::from_str(&body)?;
        let docs = run_query(req, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);
                assert_eq!(body.hits, 4)
            })
            .map_err(|_| ());

        tokio::run(docs);
        Ok(())
    }

    #[test]
    fn test_bool_query() -> Result<(), serde_json::Error> {
        let test_json = r#"{"query": { "bool": {
                "must": [ { "term": { "test_text": "document" } } ],
                "must_not": [ {"range": {"test_i64": { "gt": 2017 } } } ] } } }"#;

        let query = serde_json::from_str::<Search>(test_json)?;
        let docs = run_query(query, "test_index")
            .map(|q| {
                let body: SearchResults = wait_json(q);
                assert_eq!(body.hits, 2)
            })
            .map_err(|_| ());

        tokio::run(docs);
        Ok(())
    }
}
