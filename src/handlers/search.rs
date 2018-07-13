use super::*;

use futures::{future, Future, Stream};

use hyper::Method;
use std::collections::HashMap;
use std::io::Result as IOResult;
use std::panic::RefUnwindSafe;

#[derive(Deserialize, Debug)]
pub struct Search {
    pub query: Queries,

    #[serde(default = "default_limit")]
    pub limit: usize,
}

impl Search {
    pub fn all() -> Self {
        Search {
            query: Queries::AllQuery,
            limit: 5,
        }
    }
}

fn default_limit() -> usize { 5 }

#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum Queries {
    TermQuery { term: HashMap<String, String> },
    TermsQuery { terms: HashMap<String, Vec<String>> },
    RangeQuery { range: HashMap<String, HashMap<String, i64>> },
    RawQuery { raw: String },
    AllQuery,
}

#[derive(Clone, Debug)]
pub struct SearchHandler {
    catalog: Arc<IndexCatalog>,
}

impl RefUnwindSafe for SearchHandler {}

impl SearchHandler {
    pub fn new(catalog: Arc<IndexCatalog>) -> Self { SearchHandler { catalog } }
}

impl Handler for SearchHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        let index = IndexPath::take_from(&mut state);
        let query_options = QueryOptions::take_from(&mut state);

        match *Method::borrow_from(&state) {
            Method::Post => {
                let f = Body::take_from(&mut state).concat2().then(move |body| match body {
                    Ok(b) => {
                        let search: Search = serde_json::from_slice(&b).unwrap();
                        info!("Query: {:#?}", search);
                        let docs = match self.catalog.search_index(&index.index, &search) {
                            Ok(v) => v,
                            Err(ref e) => return handle_error(state, e),
                        };
                        info!("Query returned {} doc(s) on Index: {}", docs.len(), index.index);

                        let data = Some(if query_options.pretty {
                            (serde_json::to_vec_pretty(&docs).unwrap(), mime::APPLICATION_JSON)
                        } else {
                            (serde_json::to_vec(&docs).unwrap(), mime::APPLICATION_JSON)
                        });
                        let resp = create_response(&state, StatusCode::Ok, data);
                        future::ok((state, resp))
                    }
                    Err(ref e) => handle_error(state, e),
                });
                Box::new(f)
            }
            Method::Get => {
                let docs = match self.catalog.search_index(&index.index, &Search::all()) {
                    Ok(v) => v,
                    Err(ref e) => return Box::new(handle_error(state, e)),
                };
                info!("Query returned {} doc(s) on Index: {}", docs.len(), index.index);

                let data = Some(if query_options.pretty {
                    (serde_json::to_vec_pretty(&docs).unwrap(), mime::APPLICATION_JSON)
                } else {
                    (serde_json::to_vec(&docs).unwrap(), mime::APPLICATION_JSON)
                });
                let resp = create_response(&state, StatusCode::Ok, data);
                Box::new(future::ok((state, resp)))
            }
            _ => unimplemented!(),
        }
    }
}

new_handler!(SearchHandler);

#[cfg(test)]
mod tests {

    use index::tests::*;
    use super::*;
    use serde_json;

    #[derive(Deserialize, Debug)]
    struct TestResults {
        hits: i32,
        docs: Vec<TestDoc>,
    }

    #[derive(Deserialize, Debug)]
    struct TestDoc {
        score: f32,
        doc:   TestSchema,
    }

    #[derive(Deserialize, Debug)]
    struct TestSchema {
        test_text: Vec<String>,
        test_i64:  Vec<i64>,
        test_u64:  Vec<u64>,
    }

    #[test]
    fn test_serializing() {
        let term_query = r#"{ "query" : { "term" : { "user" : "Kimchy" } } }"#;
        let terms_query = r#"{ "query": { "terms" : { "user" : ["kimchy", "elasticsearch"] } } }"#;
        let range_query = r#"{ "query": { "range" : { "age" : { "gte" : 10, "lte" : 20 } } } }"#;
        let raw_query = r#"{ "query" : { "raw" : "year:[1 TO 10]" } }"#;

        let parsed_term: Search = serde_json::from_str(term_query).unwrap();
        let parsed_terms: Search = serde_json::from_str(terms_query).unwrap();
        let parsed_range: Search = serde_json::from_str(range_query).unwrap();
        let parsed_raw: Search = serde_json::from_str(raw_query).unwrap();
        let queries = vec![parsed_term, parsed_terms, parsed_range, parsed_raw];

        for q in queries {
            match q.query {
                Queries::TermQuery { term } => {
                    assert!(term.contains_key("user"));
                    assert_eq!(term.get("user").unwrap(), "Kimchy");
                }
                Queries::TermsQuery { terms } => {
                    assert!(terms.contains_key("user"));
                    assert_eq!(terms.get("user").unwrap().len(), 2);
                    assert_eq!(terms.get("user").unwrap()[0], "kimchy");
                }
                Queries::RangeQuery { range } => {
                    assert!(range.contains_key("age"));
                    assert_eq!(*range.get("age").unwrap().get("gte").unwrap(), 10i64);
                    assert_eq!(*range.get("age").unwrap().get("lte").unwrap(), 20i64);
                }
                Queries::RawQuery { raw } => {
                    assert_eq!(raw, "year:[1 TO 10]");
                }
                _ => (),
            }
        }
    }

    #[test]
    fn test_term_search() {
        let idx = create_test_index();
        let catalog = IndexCatalog::with_index("test_index".to_string(), idx).unwrap();
        let handler = SearchHandler::new(Arc::new(catalog));
        let client = create_test_client(handler);

        let req = client.get("http://localhost/test_index").perform().unwrap();
        assert_eq!(req.status(), StatusCode::Ok);

        let body = req.read_body().unwrap();
        let docs: TestResults = serde_json::from_slice(&body).unwrap();

        assert_eq!(docs.hits, 5);
        assert_eq!(docs.docs.len(), 5);
    }
}
