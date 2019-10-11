use std::iter::Sum;
use std::ops::Add;

use serde::{Deserialize, Serialize};

use crate::query::KeyValue;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScoredDoc<D: Clone> {
    pub score: Option<f32>,
    pub doc: D,
}

impl<D: Clone> ScoredDoc<D> {
    pub fn new(score: Option<f32>, doc: D) -> Self {
        Self { score, doc }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SearchResults<D: Clone> {
    pub hits: usize,
    pub docs: Vec<ScoredDoc<D>>,
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
    pub fn get_docs(self) -> Vec<ScoredDoc<D>> {
        self.docs
    }

    //    pub fn get_facets(&mut self) -> Vec<KeyValue<u64>> {
    //        self.facets.to_owned()
    //    }

    pub fn new(docs: Vec<ScoredDoc<D>>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            facets: Vec::new(),
        }
    }

    pub fn with_facets(docs: Vec<ScoredDoc<D>>, facets: Vec<KeyValue<String, u64>>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            facets,
        }
    }

    //    pub fn with_error(error: Error) -> Self {
    //        Self {
    //            hits: 0,
    //            docs: Vec::new(),
    //            facets: Vec::new(),
    //            error: Some(error),
    //        }
    //    }
}
