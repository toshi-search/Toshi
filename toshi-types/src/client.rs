use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ScoredDoc<D> {
    score: Option<f32>,
    doc: D,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SearchResults<D> {
    pub hits: usize,
    pub docs: Vec<ScoredDoc<D>>,
}
