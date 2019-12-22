use std::iter::Sum;
use std::ops::Add;

use serde::{Deserialize, Serialize};

use crate::query::KeyValue;

/// A single document returned from a Tantivy Index
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScoredDoc<D: Clone> {
    /// The document's relevancy score
    pub score: Option<f32>,
    /// The actual document
    pub doc: D,
}

impl<D: Clone> ScoredDoc<D> {
    /// Constructor for a new ScoredDoc
    pub fn new(score: Option<f32>, doc: D) -> Self {
        Self { score, doc }
    }
}

/// The Search response object from Toshi
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SearchResults<D: Clone> {
    /// The number of documents returned
    pub hits: usize,
    /// The actual documents, see [`ScoredDoc`]: ScoredDoc
    pub docs: Vec<ScoredDoc<D>>,
    /// The, if any, facets returned
    pub facets: Vec<KeyValue<String, u64>>,
}

impl<D: Clone> Add for SearchResults<D> {
    type Output = SearchResults<D>;

    fn add(self, mut rhs: SearchResults<D>) -> Self::Output {
        let mut docs = self.docs;
        let mut facets = self.facets;
        let hits = self.hits + rhs.hits;
        facets.append(&mut rhs.facets);
        docs.append(&mut rhs.get_docs());

        Self { hits, docs, facets }
    }
}

impl<D: Clone> Sum for SearchResults<D> {
    fn sum<I: Iterator<Item = SearchResults<D>>>(iter: I) -> Self {
        iter.fold(Self::new(Vec::new()), |r, sr| r + sr)
    }
}

impl<D: Clone> SearchResults<D> {
    /// Getter for returned documents
    pub fn get_docs(self) -> Vec<ScoredDoc<D>> {
        self.docs
    }

    /// Constructor for just documents
    pub fn new(docs: Vec<ScoredDoc<D>>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            facets: Vec::new(),
        }
    }

    /// Constructor for documents with facets
    pub fn with_facets(docs: Vec<ScoredDoc<D>>, facets: Vec<KeyValue<String, u64>>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            facets,
        }
    }
}
