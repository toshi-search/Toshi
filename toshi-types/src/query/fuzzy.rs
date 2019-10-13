use std::fmt;

use serde::{Deserialize, Serialize};
use tantivy::query::{FuzzyTermQuery, Query as TantivyQuery};
use tantivy::schema::Schema;

use crate::query::{make_field_value, CreateQuery, KeyValue};
use crate::Result;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FuzzyTerm {
    value: String,
    #[serde(default)]
    distance: u8,
    #[serde(default)]
    transposition: bool,
}

impl FuzzyTerm {
    pub fn new(value: String, distance: u8, transposition: bool) -> Self {
        Self {
            value,
            distance,
            transposition,
        }
    }
}

#[derive(Default)]
pub struct FuzzyTermBuilder {
    value: String,
    distance: u8,
    transposition: bool,
}

impl FuzzyTermBuilder {
    pub fn new() -> Self {
        FuzzyTermBuilder::default()
    }

    pub fn with_value<V>(mut self, value: V) -> Self
    where
        V: fmt::Display,
    {
        self.value = value.to_string();
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

    pub fn build(self) -> FuzzyTerm {
        FuzzyTerm::new(self.value, self.distance, self.transposition)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FuzzyQuery {
    fuzzy: KeyValue<String, FuzzyTerm>,
}

impl FuzzyQuery {
    pub fn new(fuzzy: KeyValue<String, FuzzyTerm>) -> Self {
        Self { fuzzy }
    }
}

impl CreateQuery for FuzzyQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn TantivyQuery>> {
        let KeyValue { field, value } = self.fuzzy;
        let term = make_field_value(schema, &field, &value.value)?;
        Ok(Box::new(FuzzyTermQuery::new(term, value.distance, value.transposition)))
    }
}
