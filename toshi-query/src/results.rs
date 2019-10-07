use std::collections::BTreeMap;
use std::iter::Sum;
use std::ops::Add;

use serde::{Deserialize, Serialize};
use tantivy::schema::{NamedFieldDocument, Value};

use crate::error::Error;
use crate::query::KeyValue;

#[derive(Serialize, Deserialize, Debug)]
pub struct SearchResults {
    pub hits: usize,
    #[serde(default = "Vec::new")]
    pub docs: Vec<ScoredDoc>,
    #[serde(default = "Vec::new")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub facets: Vec<KeyValue<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

impl Add for SearchResults {
    type Output = SearchResults;

    fn add(self, mut rhs: SearchResults) -> Self::Output {
        let mut docs = self.docs;
        let mut facets = self.facets;
        facets.append(&mut rhs.get_facets());
        docs.append(&mut rhs.get_docs());

        Self {
            hits: self.hits + rhs.hits,
            docs,
            facets,
            error: None,
        }
    }
}

impl Sum for SearchResults {
    fn sum<I: Iterator<Item = SearchResults>>(iter: I) -> Self {
        iter.fold(Self::with_facets(Vec::new(), Vec::new()), |r, sr| r + sr)
    }
}

impl SearchResults {
    pub fn get_docs(&mut self) -> Vec<ScoredDoc> {
        self.docs.to_owned()
    }

    pub fn get_facets(&mut self) -> Vec<KeyValue<u64>> {
        self.facets.to_owned()
    }

    pub fn new(docs: Vec<ScoredDoc>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            facets: Vec::new(),
            error: None,
        }
    }

    pub fn with_facets(docs: Vec<ScoredDoc>, facets: Vec<KeyValue<u64>>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            facets,
            error: None,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            hits: 0,
            docs: Vec::new(),
            facets: Vec::new(),
            error: Some(error),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
