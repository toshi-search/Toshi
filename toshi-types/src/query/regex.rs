use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use tantivy::query::{Query, RegexQuery as TantivyRegexQuery};
use tantivy::schema::Schema;

use crate::query::{CreateQuery, KeyValue};
use crate::{error::Error, Result};

#[derive(Serialize, Deserialize, Debug)]
pub struct RegexQuery<'a> {
    #[serde(borrow = "'a")]
    regex: KeyValue<Cow<'a, str>, Cow<'a, str>>,
}

impl<'a> CreateQuery for RegexQuery<'a> {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value, .. } = self.regex;
        let field = schema
            .get_field(&field)
            .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", field)))?;
        Ok(Box::new(TantivyRegexQuery::new(value.to_string(), field)))
    }
}
