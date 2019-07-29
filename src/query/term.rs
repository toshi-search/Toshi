use serde::{Deserialize, Serialize};
use tantivy::query::{Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema};

use crate::query::*;
use crate::Result;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ExactTerm {
    term: KeyValue<String>,
}

impl ExactTerm {
    pub fn new(term: KeyValue<String>) -> Self {
        Self { term }
    }
}

impl CreateQuery for ExactTerm {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value } = self.term;
        let term = make_field_value(schema, &field, &value)?;
        Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
    }
}
