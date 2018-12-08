use crate::query::CreateQuery;
use crate::{Error, Result};

use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use tantivy::query::{Query, RegexQuery as TantivyRegexQuery};
use tantivy::schema::Schema;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RegexQuery {
    regexp: HashMap<String, String>,
}

impl CreateQuery for RegexQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        if let Some((k, v)) = self.regexp.into_iter().take(1).next() {
            let field = schema
                .get_field(&k)
                .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", k)))?;
            Ok(Box::new(TantivyRegexQuery::new(v, field)))
        } else {
            Err(Error::QueryError("Query generation failed".into()))
        }
    }
}
