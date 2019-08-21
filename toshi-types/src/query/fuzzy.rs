use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use tantivy::query::{FuzzyTermQuery, Query};
use tantivy::schema::Schema;

use crate::query::{make_field_value, CreateQuery, KeyValue};
use crate::Result;

#[derive(Serialize, Deserialize, Debug)]
pub struct FuzzyTerm<'a> {
    value: Cow<'a, str>,
    #[serde(default)]
    distance: u8,
    #[serde(default)]
    transposition: bool,
}

impl<'a> FuzzyTerm<'a> {
    pub fn new(value: &'a str, distance: u8, transposition: bool) -> Self {
        Self {
            value: Cow::Borrowed(value),
            distance,
            transposition,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FuzzyQuery<'a> {
    #[serde(borrow = "'a")]
    fuzzy: KeyValue<Cow<'a, str>, FuzzyTerm<'a>>,
}

impl<'a> FuzzyQuery<'a> {
    pub fn new(fuzzy: KeyValue<Cow<'a, str>, FuzzyTerm<'a>>) -> Self {
        Self { fuzzy }
    }
}

impl<'a> CreateQuery for FuzzyQuery<'a> {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value } = self.fuzzy;
        let term = make_field_value(schema, &field, &value.value)?;
        Ok(Box::new(FuzzyTermQuery::new(term, value.distance, value.transposition)))
    }
}
