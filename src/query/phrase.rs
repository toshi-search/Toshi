use crate::query::*;
use crate::{Error, Result};

use serde::{Deserialize, Serialize};
use tantivy::query::{PhraseQuery as TantivyPhraseQuery, Query};
use tantivy::schema::Schema;
use tantivy::Term;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PhraseQuery {
    phrase: KeyValue<TermPair>,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TermPair {
    terms: Vec<String>,
    offsets: Option<Vec<usize>>,
}

impl TermPair {
    pub fn new(terms: Vec<String>, offsets: Option<Vec<usize>>) -> Self {
        TermPair { terms, offsets }
    }
}

impl PhraseQuery {
    pub fn new(phrase: KeyValue<TermPair>) -> Self {
        PhraseQuery { phrase }
    }
}

impl CreateQuery for PhraseQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        let KeyValue { field, value } = self.phrase;
        if let Some(offsets) = value.offsets {
            if value.terms.len() <= 1 {
                return Err(Error::QueryError("Phrase Query must have more than 1 term".into()));
            } else if value.terms.len() != offsets.len() {
                return Err(Error::QueryError(format!(
                    "Differing numbers of offsets and query terms ({} and {})",
                    value.terms.len(),
                    offsets.len()
                )));
            }
            let paired_terms = value
                .terms
                .into_iter()
                .zip(offsets.into_iter())
                .map(|(t, o)| match make_field_value(schema, &field, &t) {
                    Ok(f) => Ok((o, f)),
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
