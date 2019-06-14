use serde::{Deserialize, Serialize};

use crate::query::KeyValue;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct FacetQuery(pub KeyValue<Vec<String>>);

impl FacetQuery {
    pub fn new(facets: KeyValue<Vec<String>>) -> Self {
        Self(facets)
    }
}
