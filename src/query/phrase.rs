use query::{make_field_value, CreateQuery};
use std::collections::HashMap;
use tantivy::query::{PhraseQuery as TantivyPhraseQuery, Query};
use tantivy::schema::Schema;
use tantivy::Term;
use {Error, Result};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PhraseQuery {
    phrase: HashMap<String, TermPair>,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TermPair {
    terms: Vec<String>,
    offsets: Option<Vec<usize>>,
}

impl CreateQuery for PhraseQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        if let Some((k, v)) = self.phrase.into_iter().take(1).next() {
            if let Some(offsets) = v.offsets {
                if v.terms.len() == offsets.len() {
                    return Err(Error::QueryError(format!(
                        "Differing numbers of offsets and query terms ({} and {})",
                        v.terms.len(),
                        offsets.len()
                    )));
                }
                let paired_terms = v
                    .terms
                    .into_iter()
                    .zip(offsets.into_iter())
                    .map(|(t, o)| match make_field_value(schema, &k, &t) {
                        Ok(f) => Ok((o, f)),
                        Err(e) => Err(e),
                    }).collect::<Result<Vec<(usize, Term)>>>()?;
                Ok(Box::new(TantivyPhraseQuery::new_with_offset(paired_terms)))
            } else {
                let terms = v
                    .terms
                    .into_iter()
                    .map(|t| make_field_value(schema, &k, &t))
                    .collect::<Result<Vec<Term>>>()?;
                Ok(Box::new(TantivyPhraseQuery::new(terms)))
            }
        } else {
            Err(Error::QueryError("Query generation failed".into()))
        }
    }
}
