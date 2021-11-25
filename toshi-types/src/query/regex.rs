use serde::{Deserialize, Serialize};
use tantivy::query::{Query, RegexQuery as TantivyRegexQuery};
use tantivy::schema::Schema;

use crate::query::{CreateQuery, KeyValue};
use crate::{error::Error, Result};

/// A search query based around a regular expression
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegexQuery {
    regex: KeyValue<String, String>,
}

impl RegexQuery {
    /// Constructor for a query from a known key value
    pub fn new(regex: KeyValue<String, String>) -> Self {
        Self { regex }
    }
    /// Constructor to create a key value for the user
    pub fn from_str<R>(field: String, regex: R) -> Self
    where
        R: ToString,
    {
        Self::new(KeyValue::new(field, regex.to_string()))
    }
}

impl CreateQuery for RegexQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value, .. } = self.regex;
        let field = schema
            .get_field(&field)
            .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", field)))?;
        Ok(Box::new(TantivyRegexQuery::from_pattern(&value, field)?))
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::*;

    use super::*;

    #[test]
    fn test_valid_regex() {
        let body = r#"{ "regex": { "test_text": ".*" } }"#;
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field("test_text", FAST);
        let phrase: RegexQuery = serde_json::from_str(body).unwrap();
        let query = phrase.create_query(&schema.build());
        assert!(query.is_ok());
    }

    #[test]
    fn test_bad_regex() {
        let body = r#"{ "regex": { "test_text": "[(.!" } }"#;
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field("test_text", FAST);
        let phrase: RegexQuery = serde_json::from_str(body).unwrap();
        let query = phrase.create_query(&schema.build());
        assert!(query.is_err());
    }

    #[test]
    fn test_create_regex() {
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field("test_text", FAST);
        let phrase: RegexQuery = RegexQuery::from_str("test_text".into(), ".*");
        let query = phrase.create_query(&schema.build());

        assert!(query.is_ok());
    }
}
