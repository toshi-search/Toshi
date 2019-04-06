use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tantivy::schema::NamedFieldDocument;
use tantivy::schema::Value;
use tower_web::Response;

use crate::error::Error;
use crate::query::SummaryDoc;

#[derive(Response, Serialize, Deserialize, Debug)]
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
