use bytes::Buf;
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt};
use hyper::body::aggregate;
use hyper::Body;
use tracing::*;

use toshi_types::query::Search;

use crate::handlers::ResponseFuture;
use crate::index::SharedCatalog;
use crate::utils::{empty_with_code, with_body};
use crate::SearchResults;

#[inline]
pub fn fold_results(results: Vec<SearchResults>) -> SearchResults {
    results.into_iter().sum()
}

pub async fn doc_search(catalog: SharedCatalog, body: Body, index: String) -> ResponseFuture {
    let b = aggregate(body).await?;
    let req = serde_json::from_slice::<Search>(b.bytes()).unwrap();
    let c = catalog.lock().await;
    let req = if req.query.is_none() { Search::all_docs() } else { req };

//    if c.exists(&index) {
        info!("Query: {:?}", req);
//        let mut tasks = FuturesUnordered::new();
//        tasks.push(future::Either::Left(c.search_local_index(&index, req.clone())));
//        if c.remote_exists(&index) {
//            tasks.push(future::Either::Right(c.search_remote_index(&index, req)));
//        }
//        let mut results = vec![];
//        while let Some(Ok(r)) = tasks.next().await {
//            results.extend(r);
//        }
//
//        let response = fold_results(results);
        let result = c.search_local_index(&index, req.clone()).await.unwrap();
        Ok(with_body(result))
//    } else {
//        Ok(empty_with_code(hyper::StatusCode::NOT_FOUND))
//    }
}

pub async fn all_docs(catalog: SharedCatalog, index: String) -> ResponseFuture {
    let body = Body::from(serde_json::to_vec(&Search::all_docs()).unwrap());
    doc_search(catalog, body, index).await
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use bytes::Buf;
    use futures::TryFutureExt;
    use hyper::Response;
    use pretty_assertions::assert_eq;
    use serde::de::DeserializeOwned;
    use tokio::runtime::Runtime;

    use toshi_types::query::*;
    use toshi_types::query::{KeyValue, Query};

    use crate::handlers::ResponseFuture;
    use crate::index::tests::*;

    use super::*;

    type ReturnUnit = Result<(), hyper::error::Error>;

    pub async fn wait_json<T: DeserializeOwned>(r: Response<Body>) -> T {
        let body = hyper::body::aggregate(r.into_body()).await.unwrap();
        let body_bytes = body.bytes();
        serde_json::from_slice::<T>(body_bytes).unwrap_or_else(|e| panic!("Could not deserialize JSON: {:?}", e))
    }

    pub async fn run_query(req: Search, index: &str) -> ResponseFuture {
        let cat = create_test_catalog(index);
        doc_search(Arc::clone(&cat), Body::from(serde_json::to_vec(&req).unwrap()), index.into()).await
    }

    pub fn cmp_float(a: f32, b: f32) -> bool {
        let abs_a = a.abs();
        let abs_b = b.abs();
        let diff = (a - b).abs();
        if diff == 0.0 {
            return true;
        } else if a == 0.0 || b == 0.0 || (abs_a + abs_b < std::f32::MIN_POSITIVE) {
            return diff < (std::f32::EPSILON * std::f32::MIN_POSITIVE);
        }
        diff / (abs_a + abs_b).min(std::f32::MAX) < std::f32::EPSILON
    }

    #[test]
    fn test_term_query() {
        let term = KeyValue::new("test_text".into(), "document".into());
        let term_query = Query::Exact(ExactTerm::new(term));
        let search = Search::new(Some(term_query), None, 10);
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_query(search, "test_index").and_then(|q| {
            async {
                let body: SearchResults = wait_json(q).await;
                assert_eq!(body.hits, 3);
                Ok(())
            }
        }))
        .unwrap();
    }

    #[test]
    fn test_phrase_query() {
        let terms = TermPair::new(vec!["test".into(), "document".into()], None);
        let phrase = KeyValue::new("test_text".into(), terms);
        let term_query = Query::Phrase(PhraseQuery::new(phrase));
        let search = Search::new(Some(term_query), None, 10);
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_query(search, "test_index").and_then(|q| {
            async {
                let body: SearchResults = wait_json(q).await;
                assert_eq!(body.hits, 3);
                Ok(())
            }
        }))
        .unwrap();
    }

    #[test]
    fn test_wrong_index_error() -> ReturnUnit {
        let cat = create_test_catalog("test_index");
        let body = r#"{ "query" : { "raw": "test_text:\"document\"" } }"#;

        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            doc_search(Arc::clone(&cat), Body::from(body), "asdf".into())
                .and_then(|_| async { Ok(()) })
                .map_err(|err| {
                    assert_eq!(err.to_string(), "Unknown Index: \'asdf\' does not exist");
                    err
                }),
        )
    }

    #[test]
    fn test_bad_raw_query_syntax() -> ReturnUnit {
        let cat = create_test_catalog("test_index");
        let body = r#"{ "query" : { "raw": "asd*(@sq__" } }"#;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            doc_search(Arc::clone(&cat), Body::from(body), "test_index".into())
                .and_then(|_| async { Ok(()) })
                .map_err(|err| {
                    assert_eq!(err.to_string(), "Query Parse Error: invalid digit found in string");
                    err
                }),
        )
    }

    #[test]
    fn test_unindexed_field() -> ReturnUnit {
        let cat = create_test_catalog("test_index");
        let body = r#"{ "query" : { "raw": "test_unindex:yes" } }"#;

        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            doc_search(Arc::clone(&cat), Body::from(body), "test_index".into())
                .and_then(|r| {
                    async {
                        let docs: SearchResults = wait_json(r).await;
                        assert_eq!(docs.hits, 0);
                        Ok(())
                    }
                })
                .map_err(|err| dbg!(err)),
        )
    }

    #[test]
    fn test_bad_term_field_syntax() -> Result<(), serde_json::Error> {
        let cat = create_test_catalog("test_index");
        let body = r#"{ "query" : { "term": { "asdf": "Document" } } }"#;
        let _req: Search = serde_json::from_str(body)?;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            doc_search(Arc::clone(&cat), Body::from(body), "test_index".into())
                .and_then(|_| async { Ok(()) })
                .map_err(|_| ()),
        )
        .unwrap();
        Ok(())
    }

    #[test]
    fn test_facets() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "term": { "test_text": "document" } }, "facets": { "test_facet": ["/cat"] } }"#;
        let req: Search = serde_json::from_str(body)?;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            run_query(req, "test_index")
                .and_then(|q| {
                    async {
                        let b: SearchResults = wait_json(q).await;
                        assert_eq!(b.facets[0].value, 1);
                        assert_eq!(b.facets[1].value, 1);
                        assert_eq!(b.facets[0].field, "/cat/cat2");
                        Ok(())
                    }
                })
                .map_err(|_| ()),
        )
        .unwrap();
        Ok(())
    }

    #[test]
    fn test_raw_query() -> Result<(), serde_json::Error> {
        let body = r#"test_text:"Duckiment""#;
        let req = Search::new(Some(Query::Raw { raw: body.into() }), None, 10);
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            run_query(req, "test_index")
                .and_then(|q| {
                    async {
                        let body: SearchResults = wait_json(q).await;
                        assert_eq!(body.hits as usize, body.docs.len());
                        assert_eq!(body.docs[0].doc["test_text"][0].text().unwrap(), "Test Duckiment 3");
                        Ok(())
                    }
                })
                .map_err(|_| ()),
        )
        .unwrap();
        Ok(())
    }

    #[test]
    fn test_fuzzy_term_query() -> Result<(), serde_json::Error> {
        let fuzzy = KeyValue::new("test_text".into(), FuzzyTerm::new("document".into(), 0, false));
        let term_query = Query::Fuzzy(FuzzyQuery::new(fuzzy));
        let search = Search::new(Some(term_query), None, 10);
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            run_query(search, "test_index")
                .and_then(|q| {
                    async {
                        let body: SearchResults = wait_json(q).await;

                        assert_eq!(body.hits as usize, body.docs.len());
                        assert_eq!(body.hits, 3);
                        assert_eq!(body.docs.len(), 3);
                        Ok(())
                    }
                })
                .map_err(|_| ()),
        )
        .unwrap();
        Ok(())
    }

    #[test]
    fn test_inclusive_range_query() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gte" : 2012, "lte" : 2015 } } } }"#;
        let req: Search = serde_json::from_str(body)?;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_query(req, "test_index").and_then(|q| {
            async {
                let body: SearchResults = wait_json(q).await;

                assert_eq!(body.hits as usize, body.docs.len());
                assert_eq!(cmp_float(body.docs[0].score.unwrap(), 1.0), true);
                Ok(())
            }
        }))
        .unwrap();
        Ok(())
    }

    #[test]
    fn test_exclusive_range_query() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "range" : { "test_i64" : { "gt" : 2012, "lt" : 2015 } } } }"#;
        let req: Search = serde_json::from_str(&body)?;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            run_query(req, "test_index")
                .and_then(|q| {
                    async {
                        let body: SearchResults = wait_json(q).await;

                        assert_eq!(body.hits as usize, body.docs.len());
                        assert_eq!(cmp_float(body.docs[0].score.unwrap(), 1.0), true);
                        Ok(())
                    }
                })
                .map_err(|_| ()),
        )
        .unwrap();
        Ok(())
    }

    #[test]
    fn test_regex_query() -> Result<(), serde_json::Error> {
        let body = r#"{ "query" : { "regex" : { "test_text" : "d[ou]{1}c[k]?ument" } } }"#;
        let req: Search = serde_json::from_str(&body)?;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            run_query(req, "test_index")
                .and_then(|q| {
                    async {
                        let body: SearchResults = wait_json(q).await;
                        assert_eq!(body.hits, 4);
                        Ok(())
                    }
                })
                .map_err(|_| ()),
        )
        .unwrap();
        Ok(())
    }

    #[test]
    fn test_bool_query() -> Result<(), serde_json::Error> {
        let test_json = r#"{"query": { "bool": {
                "must": [ { "term": { "test_text": "document" } } ],
                "must_not": [ {"range": {"test_i64": { "gt": 2017 } } } ] } } }"#;

        let query = serde_json::from_str::<Search>(test_json)?;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(
            run_query(query, "test_index")
                .and_then(|q| {
                    async {
                        let body: SearchResults = wait_json(q).await;
                        assert_eq!(body.hits, 2);
                        Ok(())
                    }
                })
                .map_err(|_| ()),
        )
        .unwrap();
        Ok(())
    }
}
