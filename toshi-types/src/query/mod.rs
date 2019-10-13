use std::fmt;
use std::marker::PhantomData;

use serde::de::{DeserializeOwned, Deserializer, Error as SerdeError, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::Serializer;
use serde::{Deserialize, Serialize};
use tantivy::query::Query as TantivyQuery;
use tantivy::schema::Schema;
use tantivy::Term;

use crate::error::Error;
pub use crate::query::{
    boolean::{BoolQuery, BoolQueryBuilder},
    facet::FacetQuery,
    fuzzy::{FuzzyQuery, FuzzyTerm, FuzzyTermBuilder},
    phrase::{PhraseQuery, TermPair},
    range::{RangeQuery, RangeQueryBuilder, Ranges},
    regex::RegexQuery,
    term::ExactTerm,
};

mod boolean;
mod facet;
mod fuzzy;
mod phrase;
mod range;
mod regex;
mod term;

pub trait CreateQuery {
    fn create_query(self, schema: &Schema) -> crate::Result<Box<dyn TantivyQuery>>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Query {
    Fuzzy(FuzzyQuery),
    Exact(ExactTerm),
    Phrase(PhraseQuery),
    Regex(RegexQuery),
    Range(RangeQuery),
    Boolean { bool: BoolQuery },
    Raw { raw: String },
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Search {
    pub query: Option<Query>,
    pub facets: Option<FacetQuery>,
    #[serde(default = "Search::default_limit")]
    pub limit: usize,
}

impl Search {
    pub fn new(query: Option<Query>, facets: Option<FacetQuery>, limit: usize) -> Self {
        Search { query, facets, limit }
    }

    pub fn builder() -> SearchBuilder {
        SearchBuilder::new()
    }

    pub fn with_query(query: Query) -> Self {
        Self::new(Some(query), None, Self::default_limit())
    }

    pub fn default_limit() -> usize {
        100
    }

    pub(crate) fn all_query() -> Option<Query> {
        Some(Query::All)
    }

    pub fn all_docs() -> Self {
        Self {
            query: Self::all_query(),
            facets: None,
            limit: Self::default_limit(),
        }
    }
}

pub struct SearchBuilder {
    query: Query,
    facets: Option<FacetQuery>,
    limit: usize,
}

impl Default for SearchBuilder {
    fn default() -> Self {
        SearchBuilder::new()
    }
}

impl SearchBuilder {
    pub fn new() -> Self {
        Self {
            query: Query::All,
            facets: None,
            limit: 100,
        }
    }

    pub fn with_query(mut self, query: Query) -> Self {
        self.query = query;
        self
    }
    pub fn with_facets(mut self, facets: FacetQuery) -> Self {
        self.facets = Some(facets);
        self
    }
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
    pub fn build(self) -> Search {
        Search::new(Some(self.query), self.facets, self.limit)
    }
}

fn make_field_value(schema: &Schema, k: &str, v: &str) -> crate::Result<Term> {
    let field = schema
        .get_field(k)
        .ok_or_else(|| Error::QueryError(format!("Unknown field: {}", k)))?;
    Ok(Term::from_field_text(field, v))
}

#[derive(Debug, Clone)]
pub struct KeyValue<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    pub field: K,
    pub value: V,
}

impl<K, V> KeyValue<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    pub fn new(field: K, value: V) -> Self {
        KeyValue { field, value }
    }
}

struct KVVisitor<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    marker: PhantomData<fn() -> KeyValue<K, V>>,
}

impl<K, V> KVVisitor<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    pub fn new() -> Self {
        KVVisitor { marker: PhantomData }
    }
}

impl<'de, K, V> Visitor<'de> for KVVisitor<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Value = KeyValue<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an object with a single string value of any key name")
    }

    fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        if let Some((field, value)) = access.next_entry()? {
            if access.next_entry::<String, V>()?.is_some() {
                Err(M::Error::custom("too many values"))
            } else {
                Ok(KeyValue { field, value })
            }
        } else {
            Err(M::Error::custom("not enough values"))
        }
    }
}

impl<'de, K, V> Deserialize<'de> for KeyValue<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(KVVisitor::new())
    }
}

impl<'de, K, V> Serialize for KeyValue<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut m = serializer.serialize_map(Some(1))?;
        m.serialize_entry(&self.field, &self.value)?;
        m.end()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn test_kv_serialize() {
        let kv = KeyValue::new("test_field".to_string(), 1);
        let expected = r#"{"test_field":1}"#;
        assert_eq!(expected, serde_json::to_string(&kv).unwrap());
    }
}
