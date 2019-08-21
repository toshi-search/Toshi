use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use tantivy::query::{Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema};

use crate::query::*;
use crate::Result;

#[derive(Serialize, Deserialize, Debug)]
pub struct ExactTerm<'a> {
    #[serde(borrow = "'a")]
    term: KeyValue<Cow<'a, str>, Cow<'a, str>>,
}

impl<'a> ExactTerm<'a> {
    pub fn new(term: KeyValue<Cow<'a, str>, Cow<'a, str>>) -> Self {
        Self { term }
    }
}

impl<'a> CreateQuery for ExactTerm<'a> {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value, .. } = self.term;
        let term = make_field_value(schema, &field, &value)?;
        Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
    }
}
