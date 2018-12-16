use super::settings::Settings;
use super::{Error, Result};

use tantivy::query::Query as TantivyQuery;
use tantivy::schema::Schema;
use tantivy::Term;

pub use {
    self::aggregate::{SumCollector, SummaryDoc},
    self::bool::BoolQuery,
    self::fuzzy::{FuzzyQuery, FuzzyTerm},
    self::phrase::PhraseQuery,
    self::range::{RangeQuery, Ranges},
    self::regex::RegexQuery,
    self::term::ExactTerm,
};

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

pub trait AggregateQuery<T> {
    fn result(&self) -> Result<T>;
}

#[derive(Deserialize, Debug, PartialEq)]
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

#[derive(Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Metrics {
    SumAgg { field: String },
}

#[derive(Extract, Debug)]
pub struct Request {
    pub aggs: Option<Metrics>,
    pub query: Option<Query>,
    #[serde(default = "Settings::default_result_limit")]
    pub limit: usize,
}

impl Request {
    pub fn all_docs() -> Self {
        Self {
            aggs: None,
            query: Some(Query::All),
            limit: Settings::default_result_limit(),
        }
    }
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum TermQueries {
    Fuzzy(FuzzyQuery),
    Exact(ExactTerm),
    Phrase(PhraseQuery),
    Range(RangeQuery),
    Regex(RegexQuery),
}

fn make_field_value(schema: &Schema, k: &str, v: &str) -> Result<Term> {
    let field = schema
        .get_field(k)
        .ok_or_else(|| Error::QueryError(format!("Field: {} does not exist", k)))?;
    Ok(Term::from_field_text(field, v))
}
