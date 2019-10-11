use serde::{Deserialize, Serialize};

use crate::query::KeyValue;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FacetQuery(KeyValue<String, Vec<String>>);

impl FacetQuery {
    pub fn new(facets: KeyValue<String, Vec<String>>) -> Self {
        Self(facets)
    }

    pub fn get_facets_values(&self) -> &[String] {
        &self.0.value
    }

    pub fn get_facets_fields(&self) -> &str {
        &self.0.field
    }
}
