use serde::{Deserialize, Serialize};

use crate::query::KeyValue;

/// A faceted query, see Tantivy's docs for more information [`tantivy::collector::FacetCollector`]
/// It's also of note that this is the only query that does not implement [`crate::CreateQuery`] this
/// is because facets are collected via a different interface in Tantivy, not via the query API
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FacetQuery(KeyValue<String, Vec<String>>);

impl FacetQuery {
    /// Constructor to create a new facet query from a known key value
    pub fn new(facets: KeyValue<String, Vec<String>>) -> Self {
        Self(facets)
    }

    /// Constructor to create the key value for the user
    pub fn with_terms(field: String, terms: Vec<String>) -> Self {
        Self(KeyValue::new(field, terms))
    }

    /// Return a query's values
    pub fn get_facets_values(&self) -> &[String] {
        &self.0.value
    }

    /// Return the query's fields
    pub fn get_facets_fields(&self) -> &str {
        &self.0.field
    }
}
