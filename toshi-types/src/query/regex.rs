use serde::{Deserialize, Serialize};
use tantivy::query::{Query, RegexQuery as TantivyRegexQuery};
use tantivy::schema::Schema;

use crate::query::{CreateQuery, KeyValue};
use crate::{error::Error, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegexQuery {
    regex: KeyValue<String, String>,
}

impl RegexQuery {
    pub fn new(regex: KeyValue<String, String>) -> Self {
        Self { regex }
    }
    pub fn from_str(field: String, regex: String) -> Self {
        Self::new(KeyValue::new(field, regex))
    }
}

impl CreateQuery for RegexQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value, .. } = self.regex;
        let field = schema
            .get_field(&field)
            .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", field)))?;
        Ok(Box::new(TantivyRegexQuery::new(value, field)))
    }
}
