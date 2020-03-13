use std::iter::Sum;
use std::ops::Add;

use serde::{Deserialize, Serialize};
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::IndexMeta;

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
    docs: Vec<ScoredDoc<D>>,
    /// The, if any, facets returned
    facets: Vec<KeyValue<String, u64>>,
}

impl<D: Clone> Add for SearchResults<D> {
    type Output = SearchResults<D>;

    fn add(self, mut rhs: SearchResults<D>) -> Self::Output {
        let mut docs = self.docs;
        let mut facets = self.facets;
        let hits = self.hits + rhs.hits;
        facets.append(&mut rhs.facets);
        docs.append(&mut rhs.get_docs().to_vec());

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
    pub fn get_docs(&self) -> &[ScoredDoc<D>] {
        &self.docs
    }

    pub fn get_facets(&self) -> &[KeyValue<String, u64>] {
        &self.facets
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

/// A response gotten from the _summary route for an index
#[derive(Debug, Serialize)]
pub struct SummaryResponse {
    summaries: IndexMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    segment_sizes: Option<SearcherSpaceUsage>,
}

impl SummaryResponse {
    /// Constructor for a new summary response
    pub fn new(summaries: IndexMeta, segment_sizes: Option<SearcherSpaceUsage>) -> Self {
        Self { summaries, segment_sizes }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ScoredDoc, SearchResults};
    use std::collections::BTreeMap;

    #[test]
    fn test_add() {
        let scored = ScoredDoc::new(Some(1.0), BTreeMap::<String, String>::new());
        let scored2 = ScoredDoc::new(Some(0.5), BTreeMap::<String, String>::new());
        let results = SearchResults::new(vec![scored]);
        let results2 = SearchResults::new(vec![scored2]);
        let both = results + results2;

        assert_eq!(both.docs.len(), 2);
        assert_eq!(both.hits, 2);
    }

    #[test]
    fn test_sum() {
        let scored = ScoredDoc::new(Some(1.0), BTreeMap::<String, String>::new());
        let scored2 = ScoredDoc::new(Some(0.5), BTreeMap::<String, String>::new());
        let results = SearchResults::new(vec![scored]);
        let results2 = SearchResults::new(vec![scored2]);
        let both: SearchResults<BTreeMap<String, String>> = vec![results2, results].into_iter().sum();

        assert_eq!(both.docs.len(), 2);
        assert_eq!(both.hits, 2);
    }
}
