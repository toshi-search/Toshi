use serde::{Deserialize, Serialize};
use tantivy::query::{Query, RegexQuery as TantivyRegexQuery};
use tantivy::schema::Schema;

use crate::query::{CreateQuery, KeyValue};
use crate::{Error, Result};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RegexQuery {
    regexp: KeyValue<String>,
}

impl CreateQuery for RegexQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        let KeyValue { field, value } = self.regexp;
        let field = schema
            .get_field(&field)
            .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", field)))?;
        Ok(Box::new(TantivyRegexQuery::new(value, field)))
    }
}
