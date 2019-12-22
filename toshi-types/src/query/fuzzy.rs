use std::fmt;

use serde::{Deserialize, Serialize};
use tantivy::query::{FuzzyTermQuery, Query as TantivyQuery};
use tantivy::schema::Schema;

use crate::query::{make_field_value, CreateQuery, KeyValue, Query};
use crate::Result;

/// A query where terms can have distance between them, but still be a match
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FuzzyQuery {
    fuzzy: KeyValue<String, FuzzyTerm>,
}

impl FuzzyQuery {
    /// Constructor to create a fuzzy query from a known key value
    pub fn new(fuzzy: KeyValue<String, FuzzyTerm>) -> Self {
        Self { fuzzy }
    }
    /// Creates a builder for a fuzzy query
    pub fn builder() -> FuzzyQueryBuilder {
        FuzzyQueryBuilder::default()
    }
}

impl CreateQuery for FuzzyQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn TantivyQuery>> {
        let KeyValue { field, value } = self.fuzzy;
        let term = make_field_value(schema, &field, &value.value)?;
        Ok(Box::new(FuzzyTermQuery::new(term, value.distance, value.transposition)))
    }
}

/// A term to be considered in the query
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FuzzyTerm {
    value: String,
    #[serde(default)]
    distance: u8,
    #[serde(default)]
    transposition: bool,
}

impl FuzzyTerm {
    /// Constructor to create a fuzzy query
    pub fn new(value: String, distance: u8, transposition: bool) -> Self {
        Self {
            value,
            distance,
            transposition,
        }
    }
}

#[derive(Debug, Default)]
pub struct FuzzyQueryBuilder {
    field: String,
    value: String,
    distance: u8,
    transposition: bool,
}

impl FuzzyQueryBuilder {
    pub fn new() -> Self {
        FuzzyQueryBuilder::default()
    }

    pub fn with_value<V>(mut self, value: V) -> Self
    where
        V: fmt::Display,
    {
        self.value = value.to_string();
        self
    }

    pub fn for_field<V>(mut self, field: V) -> Self
    where
        V: fmt::Display,
    {
        self.field = field.to_string();
        self
    }

    pub fn with_distance(mut self, distance: u8) -> Self {
        self.distance = distance;
        self
    }

    pub fn with_transposition(mut self) -> Self {
        self.transposition = true;
        self
    }

    pub fn build(self) -> Query {
        let term = FuzzyTerm::new(self.value, self.distance, self.transposition);
        let query = FuzzyQuery::new(KeyValue::new(self.field, term));
        Query::Fuzzy(query)
    }
}
