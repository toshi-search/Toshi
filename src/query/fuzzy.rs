use serde::{Deserialize, Serialize};
use tantivy::query::{FuzzyTermQuery, Query};
use tantivy::schema::Schema;

use crate::query::{make_field_value, CreateQuery, KeyValue};
use crate::Result;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct FuzzyQuery {
    fuzzy: KeyValue<FuzzyTerm>,
}

impl FuzzyQuery {
    pub fn new(fuzzy: KeyValue<FuzzyTerm>) -> Self {
        Self { fuzzy }
    }
}

impl CreateQuery for FuzzyQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        let KeyValue { field, value } = self.fuzzy;
        let term = make_field_value(schema, &field, &value.value)?;
        Ok(Box::new(FuzzyTermQuery::new(term, value.distance, value.transposition)))
    }
}
