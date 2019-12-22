use std::fmt;

use serde::{Deserialize, Serialize};
use tantivy::query::{Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema};

use crate::query::*;
use crate::Result;

/// An exact term to search for
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExactTerm {
    term: KeyValue<String, String>,
}

impl ExactTerm {
    /// Constructor with a known KeyValue
    pub fn new(term: KeyValue<String, String>) -> Self {
        Self { term }
    }

    /// Constructor to create the key value for the user
    pub fn with_term<K, V>(field: K, value: V) -> Self
    where
        K: fmt::Display,
        V: fmt::Display,
    {
        Self {
            term: KeyValue::new(field.to_string(), value.to_string()),
        }
    }
}

impl CreateQuery for ExactTerm {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value, .. } = self.term;
        let term = make_field_value(schema, &field, &value)?;
        Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
    }
}
