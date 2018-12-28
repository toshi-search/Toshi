use crate::query::{make_field_value, CreateQuery};
use crate::{Error, Result};

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tantivy::query::{Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ExactTerm {
    pub term: HashMap<String, String>,
}

impl CreateQuery for ExactTerm {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        if let Some((k, v)) = self.term.into_iter().take(1).next() {
            let term = make_field_value(schema, &k, &v)?;
            Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
        } else {
            Err(Error::QueryError("Query generation failed".into()))
        }
    }
}
