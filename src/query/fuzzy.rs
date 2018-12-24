use crate::query::{make_field_value, CreateQuery};
use crate::{Error, Result};

use std::collections::HashMap;

use serde::Deserialize;
use tantivy::query::{FuzzyTermQuery, Query};
use tantivy::schema::Schema;

#[derive(Deserialize, Debug, PartialEq, Clone)]
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

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct FuzzyQuery {
    pub fuzzy: HashMap<String, FuzzyTerm>,
}

impl CreateQuery for FuzzyQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        if let Some((k, v)) = self.fuzzy.into_iter().take(1).next() {
            let term = make_field_value(schema, &k, &v.value)?;
            Ok(Box::new(FuzzyTermQuery::new(term, v.distance, v.transposition)))
        } else {
            Err(Error::QueryError("Query generation failed".into()))
        }
    }
}
