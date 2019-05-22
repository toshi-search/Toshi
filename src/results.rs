use std::collections::BTreeMap;

use http::Response;
use hyper::Body;
use serde::{Deserialize, Serialize};
use tantivy::schema::NamedFieldDocument;
use tantivy::schema::Value;

use crate::error::Error;
use crate::query::SummaryDoc;

#[derive(Serialize, Deserialize, Debug)]
pub struct SearchResults {
    pub hits: usize,
    #[serde(default = "Vec::new")]
    pub docs: Vec<ScoredDoc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<Vec<SummaryDoc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

impl SearchResults {
    pub fn new(docs: Vec<ScoredDoc>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            aggregate: None,
            error: None,
        }
    }

    pub fn with_aggregates(docs: Vec<ScoredDoc>, aggregate: Vec<SummaryDoc>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            aggregate: Some(aggregate),
            error: None,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            hits: 0,
            docs: Vec::new(),
            aggregate: None,
            error: Some(error),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ScoredDoc {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    pub doc: BTreeMap<String, Vec<Value>>,
}

impl ScoredDoc {
    pub fn new(score: Option<f32>, doc: NamedFieldDocument) -> Self {
        ScoredDoc { score, doc: doc.0 }
    }
}

#[derive(Serialize)]
pub struct ErrorResponse {
    message: String,
}

impl From<Error> for ErrorResponse {
    fn from(err: Error) -> Self {
        Self { message: err.to_string() }
    }
}

impl From<serde_json::Error> for ErrorResponse {
    fn from(err: serde_json::Error) -> Self {
        Self { message: err.to_string() }
    }
}

impl Into<Response<Body>> for ErrorResponse {
    fn into(self) -> Response<Body> {
        let body = Body::from(serde_json::to_vec(&self).unwrap());
        Response::new(body)
    }
}
