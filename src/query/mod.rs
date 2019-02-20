use std::fmt;
use std::marker::PhantomData;

use serde::de::{Deserializer, Error as SerdeError, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::Serializer;
use serde::{Deserialize, Serialize};
use tantivy::query::Query as TantivyQuery;
use tantivy::schema::Schema;
use tantivy::Term;
use tower_web::Extract;

pub use {
    self::aggregate::{SumCollector, SummaryDoc},
    self::bool::BoolQuery,
    self::fuzzy::{FuzzyQuery, FuzzyTerm},
    self::phrase::{PhraseQuery, TermPair},
    self::range::{RangeQuery, Ranges},
    self::regex::RegexQuery,
    self::term::ExactTerm,
};

use crate::settings::Settings;
use crate::{Error, Result};

mod aggregate;
mod bool;
mod fuzzy;
mod phrase;
mod range;
mod regex;
mod term;

pub trait CreateQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<TantivyQuery>>;
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum Query {
    Boolean { bool: BoolQuery },
    Fuzzy(FuzzyQuery),
    Exact(ExactTerm),
    Phrase(PhraseQuery),
    Regex(RegexQuery),
    Range(RangeQuery),
    Raw { raw: String },
    All,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum Metrics {
    SumAgg { field: String },
}

#[derive(Serialize, Extract, Deserialize, Debug, Clone)]
pub struct Request {
    pub aggs: Option<Metrics>,
    pub query: Option<Query>,
    #[serde(default = "Settings::default_result_limit")]
    pub limit: usize,
}

impl Request {
    pub fn new(query: Option<Query>, aggs: Option<Metrics>, limit: usize) -> Self {
        Request { query, aggs, limit }
    }

    pub fn all_docs() -> Self {
        Self {
            aggs: None,
            query: Some(Query::All),
            limit: Settings::default_result_limit(),
        }
    }
}

fn make_field_value(schema: &Schema, k: &str, v: &str) -> Result<Term> {
    let field = schema
        .get_field(k)
        .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", k)))?;
    Ok(Term::from_field_text(field, v))
}

#[derive(Clone, Debug, PartialEq)]
pub struct KeyValue<T> {
    pub field: String,
    pub value: T,
}

impl<T> KeyValue<T> {
    pub fn new(field: String, value: T) -> Self {
        KeyValue { field, value }
    }
}

struct KVVisitor<T> {
    marker: PhantomData<fn() -> KeyValue<T>>,
}

impl<T> KVVisitor<T> {
    pub fn new() -> Self {
        KVVisitor { marker: PhantomData }
    }
}

impl<'de, T> Visitor<'de> for KVVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = KeyValue<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an object with a single string value of any key name")
    }

    fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        if let Some((field, value)) = access.next_entry()? {
            if access.next_entry::<String, T>()?.is_some() {
                Err(M::Error::custom("too many values"))
            } else {
                Ok(KeyValue { field, value })
            }
        } else {
            Err(M::Error::custom("not enough values"))
        }
    }
}

impl<'de, T> Deserialize<'de> for KeyValue<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(KVVisitor::new())
    }
}

impl<T> Serialize for KeyValue<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut t = serializer.serialize_struct("KeyValue", 2)?;
        t.serialize_field("field", &self.field)?;
        t.serialize_field("value", &self.value)?;
        t.end()
    }
}
