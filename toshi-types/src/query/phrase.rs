use serde::{Deserialize, Serialize};
use tantivy::query::{PhraseQuery as TantivyPhraseQuery, Query};
use tantivy::schema::Schema;
use tantivy::Term;

use crate::query::{make_field_value, CreateQuery, KeyValue};
use crate::{error::Error, Result};

/// A query for a phrase of terms, see [`tantivy::query::PhraseQuery`] for more info on what
/// can be included here
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhraseQuery {
    phrase: KeyValue<String, TermPair>,
}

impl PhraseQuery {
    /// Constructor to create a phrase query from a known key value
    pub fn new(phrase: KeyValue<String, TermPair>) -> Self {
        PhraseQuery { phrase }
    }
    /// Constructor to create the key value for the user
    pub fn with_phrase(key: String, value: TermPair) -> Self {
        PhraseQuery {
            phrase: KeyValue::new(key, value),
        }
    }
}

/// The tokens used in the phrase query
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TermPair {
    terms: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offsets: Option<Vec<usize>>,
}

impl TermPair {
    /// Constructor for creating a term pair
    pub fn new(terms: Vec<String>, offsets: Option<Vec<usize>>) -> Self {
        TermPair { terms, offsets }
    }
}

impl CreateQuery for PhraseQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<dyn Query>> {
        let KeyValue { field, value } = self.phrase;
        if value.terms.len() <= 1 {
            return Err(Error::QueryError("Phrase Query must have more than 1 term".into()));
        }
        if let Some(offsets) = &value.offsets {
            if value.terms.len() != offsets.len() {
                return Err(Error::QueryError(format!(
                    "Differing numbers of offsets and query terms ({} and {})",
                    value.terms.len(),
                    offsets.len()
                )));
            }
            let paired_terms = value
                .terms
                .iter()
                .zip(offsets)
                .map(|(t, o)| match make_field_value(schema, &field, &t) {
                    Ok(f) => Ok((*o, f)),
                    Err(e) => Err(e),
                })
                .collect::<Result<Vec<(usize, Term)>>>()?;
            Ok(Box::new(TantivyPhraseQuery::new_with_offset(paired_terms)))
        } else {
            let terms = value
                .terms
                .into_iter()
                .map(|t| make_field_value(schema, &field, &t))
                .collect::<Result<Vec<Term>>>()?;
            Ok(Box::new(TantivyPhraseQuery::new(terms)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::schema::*;

    #[test]
    fn test_no_terms() {
        let body = r#"{ "phrase": { "test_u64": { "terms": [ ] } } }"#;
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field("test_u64", FAST);
        let built = schema.build();
        let query = serde_json::from_str::<PhraseQuery>(body).unwrap().create_query(&built);

        assert_eq!(query.is_err(), true);
        assert_eq!(
            query.unwrap_err().to_string(),
            "Error in query execution: 'Phrase Query must have more than 1 term'"
        );
    }

    #[test]
    fn test_diff_terms_offsets() {
        let body = r#"{ "phrase": { "test_u64": { "terms": ["asdf", "asdf2"], "offsets": [1] } } }"#;
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field("test_u64", FAST);
        let built = schema.build();
        let phrase: PhraseQuery = serde_json::from_str(body).unwrap();
        let query = phrase.create_query(&built);

        assert_eq!(query.is_err(), true);
        assert_eq!(
            query.unwrap_err().to_string(),
            "Error in query execution: 'Differing numbers of offsets and query terms (2 and 1)'"
        );
    }

    #[test]
    fn test_query() {
        let body = r#"{ "phrase": { "test_u64": { "terms": ["asdf", "asdf2"], "offsets": [1, 2] } } }"#;
        let mut schema = SchemaBuilder::new();
        schema.add_u64_field("test_u64", FAST);
        let built = schema.build();
        let phrase: PhraseQuery = serde_json::from_str(body).unwrap();
        let query = phrase.create_query(&built);

        assert_eq!(query.is_ok(), true);
        let result = query.unwrap();
        let q: &TantivyPhraseQuery = result.downcast_ref::<TantivyPhraseQuery>().unwrap();
        assert_eq!(q.phrase_terms().len(), 2);
    }
}
